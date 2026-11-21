local byte = string.byte
local char = string.char
local find = string.find
local sub = string.sub

local function skip_ws(s, i)
  local n = #s
  while i <= n do
    local b = byte(s, i)
    if b ~= 32 and b ~= 9 and b ~= 10 and b ~= 13 then
      break
    end
    i = i + 1
  end
  return i
end

local escape_map = { ['"'] = '"', ['\\'] = '\\', ['/'] = '/', b = '\b', f = '\f', n = '\n', r = '\r', t = '\t' }
local key_order = setmetatable({}, { __mode = "k" })
local is_json_array = setmetatable({}, { __mode = "k" })

local function parse_str(s, i)
  i = i + 1
  local out = {}
  while true do
    local pos = find(s, '["\\]', i)
    if not pos then
      error("json: unterminated string")
    end
    if pos > i then
      out[#out + 1] = sub(s, i, pos - 1)
    end
    local b = byte(s, pos)
    if b == 34 then
      return table.concat(out), pos + 1
    end
    local nc = sub(s, pos + 1, pos + 1)
    if nc == "" then
      error("json: unterminated escape")
    elseif nc == "u" then
      local hex = sub(s, pos + 2, pos + 5)
      local code = tonumber(hex, 16)
      if not code or #hex < 4 then
        error("json: invalid unicode escape")
      end
      if code < 0x80 then
        out[#out + 1] = char(code)
      elseif code < 0x800 then
        out[#out + 1] = char(0xC0 + math.floor(code / 0x40), 0x80 + code % 0x40)
      else
        out[#out + 1] = char(
          0xE0 + math.floor(code / 0x1000),
          0x80 + math.floor(code / 0x40) % 0x40,
          0x80 + code % 0x40
        )
      end
      i = pos + 6
    else
      out[#out + 1] = escape_map[nc] or nc
      i = pos + 2
    end
  end
end

local parse_value

local function parse_object(s, i)
  i = skip_ws(s, i + 1)
  local obj, order = {}, {}
  key_order[obj] = order
  if byte(s, i) == 125 then return obj, i + 1 end -- '}'
  while true do
    i = skip_ws(s, i)
    if byte(s, i) ~= 34 then
      error("json: expected string key near " .. i)
    end
    local key
    key, i = parse_str(s, i)
    i = skip_ws(s, i)
    if byte(s, i) ~= 58 then -- ':'
      error("json: expected ':' near " .. i)
    end
    i = skip_ws(s, i + 1)
    local value
    value, i = parse_value(s, i)
    obj[key] = value
    order[#order + 1] = key
    i = skip_ws(s, i)
    local b = byte(s, i)
    if b == 44 then -- ','
      i = i + 1
    elseif b == 125 then -- '}'
      return obj, i + 1
    else
      error("json: expected ',' or '}' near " .. i)
    end
  end
end

local function parse_array(s, i)
  i = skip_ws(s, i + 1)
  local arr = {}
  is_json_array[arr] = true
  if byte(s, i) == 93 then return arr, i + 1 end -- ']'
  while true do
    i = skip_ws(s, i)
    local value
    value, i = parse_value(s, i)
    arr[#arr + 1] = value
    i = skip_ws(s, i)
    local b = byte(s, i)
    if b == 44 then -- ','
      i = i + 1
    elseif b == 93 then -- ']'
      return arr, i + 1
    else
      error("json: expected ',' or ']' near " .. i)
    end
  end
end

parse_value = function(s, i)
  i = skip_ws(s, i)
  local b = byte(s, i)
  if b == 34 then return parse_str(s, i) -- '"'
  elseif b == 123 then return parse_object(s, i) -- '{'
  elseif b == 91 then return parse_array(s, i) -- '['
  elseif sub(s, i, i + 3) == "true" then return true, i + 4
  elseif sub(s, i, i + 4) == "false" then return false, i + 5
  elseif sub(s, i, i + 3) == "null" then return nil, i + 4
  else
    local num = s:match("^%-?%d+%.?%d*[eE]?[%+%-]?%d*", i)
    if not num or num == "" then error("json: unexpected char near " .. i) end
    return tonumber(num), i + #num
  end
end

local function json_decode(s) return (parse_value(s, 1)) end

local function encode_string(s)
  return '"' .. s:gsub('[%c"\\]', function(c)
    if c == '"' then return '\\"'
    elseif c == '\\' then return '\\\\'
    elseif c == '\n' then return '\\n'
    elseif c == '\r' then return '\\r'
    elseif c == '\t' then return '\\t'
    else return string.format('\\u%04x', c:byte()) end
  end) .. '"'
end

local function encode_value(v)
  local t = type(v)
  if v == nil then return "null"
  elseif t == "boolean" or t == "number" then return tostring(v)
  elseif t == "string" then return encode_string(v)
  elseif t == "table" then
    local parts = {}
    if is_json_array[v] then
      if #v == 0 then return "[]" end
      for idx = 1, #v do
        parts[idx] = encode_value(v[idx])
      end
      return "[" .. table.concat(parts, ",") .. "]"
    end
    local order, seen = key_order[v] or {}, {}
    for _, k in ipairs(order) do
      seen[k] = true
      if v[k] ~= nil then
        parts[#parts + 1] = encode_string(k) .. ":" .. encode_value(v[k])
      end
    end
    for k, item in pairs(v) do
      if not seen[k] then
        parts[#parts + 1] = encode_string(k) .. ":" .. encode_value(item)
      end
    end
    if #parts == 0 then return "{}" end
    return "{" .. table.concat(parts, ",") .. "}"
  end
  error("json: cannot encode type " .. t)
end

local FULL_SCOPE = "hazard:read:full"
local FILTERED_FIELD = { "sourceRefId", "latitude", "longitude", "attributes" }

local CACHE_TTL_SEC = 2
local CACHE_MAX_ENTRIES = 500
local response_cache = {}
local cache_order = {}
local cache_head = 1
local cache_tail = 0
local cache_size = 0

local function is_filtered_path(path)
  return path ~= nil and find(path, "^/aggregate/hazards") ~= nil
end

local function is_cacheable_path(path)
  return path == "/aggregate/hazards" or (path ~= nil and find(path, "^/aggregate/hazards%?") ~= nil)
end

local function cache_get(key, now)
  local entry = response_cache[key]
  if not entry then
    return nil
  end
  if entry.expires_at <= now then
    response_cache[key] = nil
    cache_size = cache_size - 1
    return nil
  end
  return entry.body
end

local function cache_set(key, body, now)
  local existing = response_cache[key]
  if existing then
    existing.body = body
    existing.expires_at = now + CACHE_TTL_SEC
    return
  end

  while cache_size >= CACHE_MAX_ENTRIES and cache_head <= cache_tail do
    local old_key = cache_order[cache_head]
    cache_order[cache_head] = nil
    cache_head = cache_head + 1
    if old_key and response_cache[old_key] then
      response_cache[old_key] = nil
      cache_size = cache_size - 1
    end
  end

  if cache_head > 1000 and cache_head > cache_tail then
    cache_head = 1
    cache_tail = 0
  end

  cache_tail = cache_tail + 1
  cache_order[cache_tail] = key
  response_cache[key] = {
    body = body,
    expires_at = now + CACHE_TTL_SEC,
  }
  cache_size = cache_size + 1
end

local function strip(hazard)
  for _, field in ipairs(FILTERED_FIELD) do hazard[field] = nil end
  return hazard
end

local function fail_closed(response_handle, body, reason)
  local err_body = '{"error":"response filtering failed"}'
  response_handle:logErr("field_filter: " .. reason)
  response_handle:headers():replace(":status", "500")
  response_handle:headers():replace("content-type", "application/json")
  if body then
    body:setBytes(err_body)
    response_handle:headers():replace("content-length", tostring(#err_body))
  else
    response_handle:headers():replace("content-length", "0")
  end
end

function envoy_on_request(request_handle)
  local path = request_handle:headers():get(":path") or ""
  local method = request_handle:headers():get(":method") or ""
  local info = request_handle:streamInfo()
  local meta = info:dynamicMetadata()
  local jwt = meta:get("envoy.filters.http.jwt_authn")
  local scope = jwt and jwt["jwt_payload"] and jwt["jwt_payload"]["scope"] or ""

  meta:set("field_filter", "path", path)
  meta:set("field_filter", "method", method)
  meta:set("field_filter", "scope", scope)

  if method == "GET" and is_cacheable_path(path) and scope ~= "" then
    local cached_body = cache_get(scope .. "|" .. path, os.time())
    if cached_body then
      meta:set("field_filter", "cache_hit", "1")
      request_handle:respond(
        { [":status"] = "200", ["content-type"] = "application/json" },
        cached_body
      )
    end
  end
end

function envoy_on_response(response_handle)
  local info = response_handle:streamInfo()
  local stash = info:dynamicMetadata():get("field_filter")
  if stash and stash["cache_hit"] == "1" then return end

  local path = stash and stash["path"]
  if not is_filtered_path(path) then return end

  local status = response_handle:headers():get(":status")
  if status ~= "200" then return end

  local method = stash and stash["method"] or ""
  local jwt = info:dynamicMetadata():get("envoy.filters.http.jwt_authn")
  local scope = (jwt and jwt["jwt_payload"] and jwt["jwt_payload"]["scope"])
    or (stash and stash["scope"])
    or ""

  local content_type = response_handle:headers():get("content-type") or ""
  local body = response_handle:body()

  if scope == FULL_SCOPE then
    if method == "GET" and is_cacheable_path(path) and find(content_type, "application/json", 1, true) and body then
      local raw = body:getBytes(0, body:length())
      cache_set(scope .. "|" .. path, raw, os.time())
    end
    return
  end

  if not find(content_type, "application/json", 1, true) or not body then
    fail_closed(response_handle, body, "missing JSON body or content-type for filtered scope=" .. tostring(scope))
    return
  end

  local raw = body:getBytes(0, body:length())
  local ok, data = pcall(json_decode, raw)
  if not ok or type(data) ~= "table" then
    local preview = sub(raw, 1, 60):gsub("[^%g ]", ".")
    fail_closed(
      response_handle,
      body,
      "decode failed: " .. tostring(data) .. " | len=" .. tostring(#raw) .. " | head=" .. preview
    )
    return
  end

  if data.hazards then
    for _, h in ipairs(data.hazards) do strip(h) end
  else
    strip(data)
  end

  local ok2, new_body = pcall(encode_value, data)
  if not ok2 then
    fail_closed(response_handle, body, "encode failed: " .. tostring(new_body))
    return
  end

  body:setBytes(new_body)
  response_handle:headers():replace("content-length", tostring(#new_body))

  if method == "GET" and is_cacheable_path(path) and scope ~= "" then
    cache_set(scope .. "|" .. path, new_body, os.time())
  end
end

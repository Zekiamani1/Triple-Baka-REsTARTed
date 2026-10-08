local function skip_ws(s, i)
    while i <= #s do
        local c = s:sub(i, i)
        if c ~= " " and c ~= " " and c ~= "\t" and c ~= "\n" and c ~= "\r" then break end
        i = i + 1
    end
    return i
end

local escape_map = { ['"']='"', ['\\']='\\', ['/']='/', b='\b', f='\f', n='\n', r='\r', t='\t' }
local key_order = setmetatable({}, { __mode = "k" })
local is_json_array = setmetatable({}, { __mode = "k" })

local function parse_str(s, i)
    i = i + 1
  local out = {}
  while true do
    local c = s:sub(i, i)
    if c == '"' then
      return table.concat(out), i + 1
    elseif c == '\\' then
      local nc = s:sub(i + 1, i + 1)
      if nc == 'u' then
        local code = tonumber(s:sub(i + 2, i + 5), 16) or 0
        if code < 0x80 then
          out[#out + 1] = string.char(code)
        elseif code < 0x800 then
          out[#out + 1] = string.char(0xC0 + math.floor(code / 0x40), 0x80 + code % 0x40)
        else
          out[#out + 1] = string.char(
            0xE0 + math.floor(code / 0x1000),
            0x80 + math.floor(code / 0x40) % 0x40,
            0x80 + code % 0x40)
        end
        i = i + 6
      else
        out[#out + 1] = escape_map[nc] or nc
        i = i + 2
      end
    elseif c == "" then
      error("json: unterminated string")
    else
      out[#out + 1] = c
      i = i + 1
    end
  end
end


local parse_value

local function parse_object(s, i)
  i = skip_ws(s, i + 1)
  local obj, order = {}, {}
  key_order[obj] = order
  if s:sub(i, i) == "}" then return obj, i + 1 end
  while true do
    i = skip_ws(s, i)
    local key
    key, i = parse_str(s, i)
    i = skip_ws(s, i) + 1 -- skip ':'
    i = skip_ws(s, i)
    local value
    value, i = parse_value(s, i)
    obj[key] = value
    order[#order + 1] = key
    i = skip_ws(s, i)
    local c = s:sub(i, i)
    if c == "," then i = i + 1
    elseif c == "}" then return obj, i + 1
    else error("json: expected ',' or '}' near " .. i) end
  end
end

local function parse_array(s, i)
  i = skip_ws(s, i + 1)
  local arr = {}
  is_json_array[arr] = true
  if s:sub(i, i) == "]" then return arr, i + 1 end
  while true do
    i = skip_ws(s, i)
    local value
    value, i = parse_value(s, i)
    arr[#arr + 1] = value
    i = skip_ws(s, i)
    local c = s:sub(i, i)
    if c == "," then i = i + 1
    elseif c == "]" then return arr, i + 1
    else error("json: expected ',' or ']' near " .. i) end
  end
end

parse_value = function(s, i)
  i = skip_ws(s, i)
  local c = s:sub(i, i)
  if c == '"' then return parse_str(s, i)
  elseif c == "{" then return parse_object(s, i)
  elseif c == "[" then return parse_array(s, i)
  elseif s:sub(i, i + 3) == "true" then return true, i + 4
  elseif s:sub(i, i + 4) == "false" then return false, i + 5
  elseif s:sub(i, i + 3) == "null" then return nil, i + 4
  else
    local num = s:match("^%-?%d+%.?%d*[eE]?[%+%-]?%d*", i)
    if not num or num == "" then error("json: unexpected char near " .. i) end
    return tonumber(num), i + #num
  end
end

local function json_decode(s) return (parse_value(s, 1)) end

local function is_array(t)
  local n = 0
  for _ in pairs(t) do n = n + 1 end
  return n == #t
end

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

-- Pretty-printed to match the transcoder's style: 1-space indent, "key": value
local function encode_value(v, indent)
  indent = indent or ""
  local t = type(v)
  if v == nil then return "null"
  elseif t == "boolean" or t == "number" then return tostring(v)
  elseif t == "string" then return encode_string(v)
  elseif t == "table" then
    local inner = indent .. " "
    local parts = {}
    if is_json_array[v] then
      if #v == 0 then return "[]" end
      for _, item in ipairs(v) do
        parts[#parts + 1] = inner .. encode_value(item, inner)
      end
      return "[\n" .. table.concat(parts, ",\n") .. "\n" .. indent .. "]"
    end
    -- object: original key order first, then any keys not seen at parse time
    local order, seen = key_order[v] or {}, {}
    for _, k in ipairs(order) do
      seen[k] = true
      if v[k] ~= nil then
        parts[#parts + 1] = inner .. encode_string(k) .. ": " .. encode_value(v[k], inner)
      end
    end
    for k, item in pairs(v) do
      if not seen[k] then
        parts[#parts + 1] = inner .. encode_string(k) .. ": " .. encode_value(item, inner)
      end
    end
    if #parts == 0 then return "{}" end
    return "{\n" .. table.concat(parts, ",\n") .. "\n" .. indent .. "}"
  end
  error("json: cannot encode type " .. t)
end

local FULL_SCOPE = "hazard:read:full"
local FILTERED_FIELD = { "sourceRefId", "latitude", "longitude", "attributes" }

local function strip(hazard)
    for _, field in ipairs(FILTERED_FIELD) do hazard[field] = nil end
    return hazard
end

function envoy_on_request(request_handle)
    local path = request_handle:headers():get(":path") or ""
    request_handle:streamInfo():dynamicMetadata():set("field_filter", "path", path)
end

function envoy_on_response(response_handle)
  local info = response_handle:streamInfo()
  local stash = info:dynamicMetadata():get("field_filter")
  local path = stash and stash["path"]
  response_handle:logInfo("field_filter: path=" .. tostring(path))
  if not path or not path:find("^/aggregate/hazards") then return end

  local jwt = info:dynamicMetadata():get("envoy.filters.http.jwt_authn")
  local scope = jwt and jwt["jwt_payload"] and jwt["jwt_payload"]["scope"]
  response_handle:logInfo("field_filter: scope=" .. tostring(scope))
  if scope == FULL_SCOPE then return end

  local content_type = response_handle:headers():get("content-type") or ""
  response_handle:logInfo("field_filter: type=" .. tostring(content_type))
  if not content_type:find("application/json") then return end

  local body = response_handle:body()
  local raw = body:getBytes(0, body:length())

  local ok, data = pcall(json_decode, raw)
  if not ok or type(data) ~= "table" then
    -- show printable bytes only; binary shows up as dots
    local preview = raw:sub(1, 60):gsub("[^%g ]", ".")
    response_handle:logErr("field_filter: decode failed: " .. tostring(data)
      .. " | len=" .. tostring(len) .. " | head=" .. preview)
    return
  end

  if data.hazards then
    for _, h in ipairs(data.hazards) do strip(h) end
  else
    strip(data) -- GetHazard: a single object, not wrapped in { hazards: [...] }
  end

  local ok2, new_body = pcall(encode_value, data)
  if not ok2 then
    response_handle:logErr("field_filter: failed to re-encode response body, passing through unfiltered")
    return
  end

  response_handle:body():setBytes(new_body)
  response_handle:headers():replace("content-length", tostring(#new_body))
end
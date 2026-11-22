// Fail startup if MongoDB rejects a configuration command.
module.exports = function command(result) {
  if (result.ok !== 1) throw new Error(JSON.stringify(result));
  return result;
};

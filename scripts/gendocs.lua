local docgen = require'docgen'

local function gen()
  local input_files = {
    './lua/sql.lua',
    './lua/sql/stmt.lua'
  }

  local output_file = "./doc/sql.txt"
  local output_file_handle = io.open(output_file, "w")

  for _, input_file in ipairs(input_files) do
    docgen.write(input_file, output_file_handle)
  end

  output_file_handle:write(" vim:tw=78:ts=8:ft=help:norl:\n")
  output_file_handle:close()
  vim.cmd [[checktime]]
end

gen()

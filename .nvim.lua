local conform = require("conform")
local util = require("conform.util")

conform.formatters.prettier_jsonc = {
	cwd = util.root_file({ ".gitignore" }),
	command = "prettier",
	args = { "--parser=json" },
}

conform.formatters_by_ft.jsonc = { "prettier_jsonc" }

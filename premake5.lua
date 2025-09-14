---@diagnostic disable: undefined-global
local LOCAL_DIR = _SCRIPT_DIR

project "lrust"
    kind "Makefile"

    filter "configurations:release"
        buildcommands {
            string.format("{CHDIR} %s;cargo build --release", LOCAL_DIR),
            string.format("{COPYFILE} %s/target/release/librust.dylib %s ", LOCAL_DIR, MOON_DIR.."/clib/rust.dylib"),
            string.format("{COPY} %s/lualib/* %s ", LOCAL_DIR, MOON_DIR.."/lualib/ext/"),
        }
    filter "configurations:debug"
        buildcommands {
            string.format("{CHDIR} %s;cargo build", LOCAL_DIR),
            string.format("{COPYFILE} %s/target/debug/librust.dylib %s ", LOCAL_DIR, MOON_DIR.."/clib/rust.dylib"),
            string.format("{COPY} %s/lualib/* %s ", LOCAL_DIR, MOON_DIR.."/lualib/ext/"),
        }
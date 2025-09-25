---@diagnostic disable: undefined-global
local LOCAL_DIR = _SCRIPT_DIR
if os.host() == "windows" then
    LOCAL_DIR = path.translate(LOCAL_DIR)
end

project "lrust"
    kind "Makefile"

    filter "configurations:release"
        buildcommands {
            -- cargo build --no-default-features --features="http,websocket"
            string.format("{CHDIR} %s && cargo build --release", LOCAL_DIR),
            string.format("{COPYFILE} %s/target/release/%s %s ", LOCAL_DIR, get_sharedlib_name("librust"), MOON_DIR.."/clib/"..get_sharedlib_name("rust")),
            string.format("{COPY} %s/lualib/* %s ", LOCAL_DIR, MOON_DIR.."/lualib/ext/"),
        }
    filter "configurations:debug"
        buildcommands {
            -- cargo build --no-default-features --features="http,websocket"
            string.format("{CHDIR} %s && cargo build", LOCAL_DIR),
            string.format("{COPYFILE} %s/target/debug/%s %s ", LOCAL_DIR, get_sharedlib_name("librust"), MOON_DIR.."/clib/"..get_sharedlib_name("rust")),
            string.format("{COPY} %s/lualib/* %s ", LOCAL_DIR, MOON_DIR.."/lualib/ext/"),
        }
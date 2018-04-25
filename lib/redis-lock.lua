local uuid = require('resty.jit-uuid')
uuid.seed()
local _M = {}

function _M:new(options)
    local options = options or {}
    return setmetatable(options, {__index = _M})
end

--[[
   acquire_lock with timeout
--]]
function _M:acquire_lock(conn, lockname, acquire_timeout, lock_timeout)
    local identifier = ngx.re.gsub(uuid(),"-", "")
    local end_time = ngx.time() + acquire_timeout
    while ngx.time() < end_time do
        if lock_timeout then
            if conn:set("lock:" .. lockname, identifier, "EX", lock_timeout, "NX") == "OK" then
                return identifier
            end
        else
            if conn:setnx("lock:" .. lockname, identifier) == "OK" then
                return identifier
            end
        end
        ngx.sleep(0.01)
    end
    ngx.log(ngx.ERR, "acquire timeout:", identifier)
    return false
end

--[[
    release_lock
--]]
function _M:release_lock(conn, lockname, identifier)
    local lockname = "lock:" .. lockname
    while true do
        conn:watch(lockname)
        if conn:get(lockname) == identifier then
            if conn:multi() == "OK" then
                conn:del(lockname)
                local results = conn:exec()
                if results and results[1] == 1 then --multi execute success
                    return true
                end
            end
        end
        conn:unwatch(lockname)
        break
    end
    return false
end

return _M

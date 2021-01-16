local t = {}
t.__index = t

function t:__run(func)
  if self.db.closed then
    return self.db:with_open(function()
      return func() -- shoud pass tbl name?
    end)
  else
   return func()
  end
end

function t:__clear_cache(succ, change)
  if succ then self.cache = {} end
end

function t:__fill_cache(query, res)
 return table.insert(self.cache, {query, res}) -- should be kv instead?
end

function t:__get_from_cache(query)
  for _, v in ipairs(self.cache) do -- is there a better way
    if v[1] == query then
      return v[2]
    end
  end
end

function t:new(db, tbl)
  local o = {}
  o.cache = {}
  o.db = db
  o.tbl = tbl

  setmetatable(o, self)

  o:__run(function()
    o.tbl_exists = o.db:exists(o.tbl)
    o.has_content = o.tbl_exists and o:count() ~= 0 or false
  end)
  return o
end

--- Create or change {self.tbl} schema. If no {schema} is given,
--- then it return current the used schema.
---@param schema table: table schema definition
---@return table: list of keys or keys and their type.
function t:schema(schema)
  local res
  return self:__run(function()
    if not schema then
      return self.tbl_exists and self.db:schema(self.tbl) or {}
    elseif not self.tbl_exists or schema.ensure then
      res = self.db:create(self.tbl, schema)
      self.tbl_exists = res
      return res
    else -- maybe better to use alter
      res = self.db:drop(self.tbl)
      res = res and self.db:create(self.tbl, schema) or false
      return res
    end
  end)
end

--- Same functionalities as |sql:drop()|, if the table is already drooped
--- then it returns false
---@return boolean
function t:drop()
  if not self.tbl_exists then return false end
  return self:__run(function()
    local res = self.db:drop(self.tbl)
    if res then self.tbl_exists = false end
    return res
  end)
end


--- Query the table and return results. If the {query} has been ran before, then
--- query results from cache will be returned.
---@param query table: query.where, query.keys, query.join
---@return table: empty table if no result
---@see sql:select()
function t:get(query)
  local cache = self:__get_from_cache(query)
  if cache then return cache end

  return self:__run(function()
    local res = self.db:select(self.tbl, query)
    if res then self:__fill_cache(query, res) end
    return res
  end)
end

--- Same functionalities as |t:get()| but with using where key only.
---@param where table: kv pairs to match against.
---@see t:__run()
---@see sql:select()
---@return boolean
function t:where(where)
  local query = {where = where}
  local cache = self:__get_from_cache(query)
  if cache then return cache end

  return self:__run(function()
    local res = self.db:select(self.tbl, query)
    if res then self:__fill_cache(query, res) end
    return res
  end)
end

--- Iterate over {self.tbl} rows and execute {func}.
---@param query table: query.where, query.keys, query.join
---@param func function: a function that expects a row
function t:each(query, func)
  if not func then return end -- or should we error out?
  local cache = self:__get_from_cache(query)
  local rows = cache and cache or (function()
    local res = self.db:select(self.tbl, query)
    if res then self:__fill_cache(query, res) end
    return res
  end)
  if not rows then return end
  for _, row in ipairs(rows) do
    func(row)
  end
end

function t:map(query, func)

end

--- Same functionalities as |sql:insert()|
---@param rows table: a row or a group of rows
---@see t:__run()
---@see sql:insert()
---@return boolean
function t:insert(rows)
  return self:__run(function()
    local succ = self.db:insert(self.tbl, rows)
    self:__clear_cache(succ, rows)
    return succ
  end)
end

--- Same functionalities as |sql:delete()|
---@param where table: opts.where
---@see t:__run()
---@see sql:delete()
---@return boolean
function t:remove(where)
  return self:__run(function()
    local succ = self.db:remove(self.tbl, {where = where})
    self:__clear_cache({where = where}, succ)
    return succ
  end)
end

--- Same functionalities as |sql:update()|
---@param specs table: a table or a list of tables with where and values keys.
---@see t:__run()
---@see sql:update()
---@return boolean
function t:update(specs)
  return self:__run(function()
    local succ = self.db:update(self.tbl, specs)
    self:__clear_cache(succ, specs)
    return succ
  end)
end

--- Same functionalities as |t:add()|, but replaces {self.tbl} content with {rows}
---@param rows table: a row or a group of rows
---@see t:__run()
---@see sql:delete()
---@see sql:insert()
---@return boolean
function t:replace(rows)
  return self:__run(function()
    self.db:delete(self.tbl)
    local succ = self.db:insert(self.tbl, rows)
    self:__clear_cache(succ, rows)
    return succ
  end)
end

--- Predicate that returns true if the table is empty
---@return boolean
function t:empty() return not self.has_content end

--- Predicate that returns true if the table exists
---@return boolean
function t:exists() return self.tbl_exists end

--- same as |sql:count|
---@return number: number of rows in {self.tbl}
function t:count()
  if not self.tbl_exists then return end
  return self:__run(function()
    return self.db:eval("select count(*) from " .. self.tbl)
  end)
end

return t

local sql = require'sql'
local eq = assert.are.same
local demo = {
  { a = 1,  b = "lsf", c = "de" },
  { a = 99, b = "sdj", c = "in" },
  { a = 32, b = "sbs", c = "en" },
  { a = 12, b = "srd", c = "fn" },
  { a = 35, b = "cba", c = "qa" },
  { a = 4,  b = "pef", c = "ru" },
  { a = 4,  b = "sam", c = "da" },
}

local dbpath = "/tmp/tbl_methods_test.sql"
local db = sql.new(dbpath)

local seed = function()
  db:open()
  db:create("T", { a = "integer", b = "text", c = "text" })
  db:insert("T", demo)
  return db:table("T"), db:table("N")
end

local clean = function()
  db:close()
  vim.loop.fs_unlink(dbpath)
end

describe('table', function()
  local t1, t2 = seed()

  describe(':new', function()
    it('create new object with sql.table methods.', function()
      eq("table", type(t1))
    end)
    it('registers table name in self.tbl.', function()
      eq("T", t1.tbl)
    end)
    it('registers whether the table exists in self.tbl_exists.', function()
      eq(true, t1.tbl_exists)
    end)
    it('registers whether the table has content in self.has_content.', function()
      eq(true, t1.has_content, 'should be false')
    end)
    it('initalizes empty cache in self.cache', function()
      eq({}, t1.cache, 'should be empty.')
    end)
    it("doesn't fail if table isn't created yat.", function()
      eq("table", type(t2))
    end)
  end)

  describe(':schema', function()
    it('returns schema if self.tbl exists', function()
      eq({ a = "integer", b = "text", c = "text" }, t1:schema())
    end)
    it("returns empty table if schema doesn't exists", function()
      eq({}, t2:schema())
    end)
    it("creates new table with schema", function()
      local schema = { id = "int", a = "text", d = "text" }
      eq(true, t2:schema(schema), "Should return true")
      eq(schema, t2:schema(), "should return the schema.")
      eq(true, t2.tbl_exists, "should alter self.exists value")
    end)
    it("should drop and recreate the table if not schema.ensure", function()
      local new = { id = "int", a = "text", f = "text" }
      eq(true, t2:schema(new), "Should return true")
      eq(new, t2:schema(), "should return the schema.")
      eq(true, t2.tbl_exists, "should alter self.exists value")
    end)
  end)

  describe(":drop", function()
    it("should drop table", function()
      eq(true, t2:drop(), "should return true")
      eq(false, t2:exists(), "should not exists.")
    end)
    it("should return false if already dropped", function()
      eq(false, t2:drop(), "should be false")
    end)
  end)

  describe(":get", function()
    it("run a query and returns results (where)", function()
      local res = t1:get{ where = {a = 1} }
      eq(demo[1], res[1], "should be identical")
    end)

    it("run a query and returns results (where & keys)", function()
      local res = t1:get{ keys = { "b", "c" }, where = { a = 1 } }
      demo[1].a = nil
      eq(demo[1], res[1], "should be identical")
    end)

    it("run a query from cache.", function()
      local res = t1:get{ keys = { "b", "c" }, where = { a = 1 } }
      demo[1].a = nil

      -- eq(demo[1], t1.cache, "should be identical")
      eq(demo[1], res[1], "should be identical")
    end)

    it("run a query and returns results when connection is closed.", function()
      db:close()
      local res = t1:get{ where = { a = 99 } }
      eq(demo[2], res[1], "should be identical")
    end)

    it("the db should stay closed.", function()
      eq(true, t1.db.closed, "should update the state")
      eq(true, db.closed, "should update the state")
    end)
  end)

  clean()
end)


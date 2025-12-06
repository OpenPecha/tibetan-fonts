-- gt_page1.lua  (kern+attribute markers, page-1 only, LaTeX-safe)

local node = node
local hlist_id = node.id("hlist")
local vlist_id = node.id("vlist")
local kern_id  = node.id("kern")

local done = false
local breakfile = nil

local function collect_markers_in_hlist(h)
  local ids = {}
  for n in node.traverse(h.head) do
    if n.id == kern_id then
      local v = node.get_attribute(n, tex.attribute.syllattr)
      if v then ids[#ids+1] = v end
    end
  end
  return ids
end

local function collect_lines_and_markers(head, lines)
  for n in node.traverse(head) do
    if n.id == hlist_id then
      local ids = collect_markers_in_hlist(n)
      if #ids > 0 then
        lines[#lines+1] = ids
      end
    elseif n.id == vlist_id and n.head then
      collect_lines_and_markers(n.head, lines)
    end
  end
end

local function gt_pre_output_filter(head)
  if done then return head end

  if not breakfile then
    local t = tex.toks["gtbreakfile"]
    if t then breakfile = (t:gsub("^%s*(.-)%s*$", "%1")) end
  end
  if not breakfile or breakfile == "" then
    return head
  end

  local lines = {}
  collect_lines_and_markers(head, lines)

  local f = io.open(breakfile, "w")
  for _,ids in ipairs(lines) do
    for i=1,#ids do
      if i > 1 then f:write(" ") end
      f:write(ids[i])
    end
    f:write("\n")
  end
  f:close()

  done = true
  return head
end

if luatexbase and luatexbase.add_to_callback then
  luatexbase.add_to_callback("pre_output_filter", gt_pre_output_filter, "gt_page1_breaks")
else
  callback.register("pre_output_filter", gt_pre_output_filter)
end

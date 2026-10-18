// Runs before first paint so a saved theme choice never flashes.
try { var t = localStorage.getItem("de-theme"); if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t); } catch (e) {}

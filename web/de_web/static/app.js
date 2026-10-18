(function () {
  var root = document.documentElement;

  // theme toggle
  var tb = document.getElementById("theme-btn");
  if (tb) tb.addEventListener("click", function () {
    var cur = root.getAttribute("data-theme") ||
      (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    var next = cur === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("de-theme", next); } catch (e) {}
  });

  // mobile course menu
  var mb = document.getElementById("menu-btn"), scrim = document.getElementById("scrim");
  function setMenu(open) {
    document.body.classList.toggle("menu-open", open);
    if (mb) mb.setAttribute("aria-expanded", open ? "true" : "false");
  }
  if (mb) mb.addEventListener("click", function () { setMenu(!document.body.classList.contains("menu-open")); });
  if (scrim) scrim.addEventListener("click", function () { setMenu(false); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") setMenu(false); });

  // sidebar filter
  var f = document.getElementById("side-filter");
  if (f) f.addEventListener("input", function () {
    var q = f.value.trim().toLowerCase();
    document.querySelectorAll(".side li[data-filter]").forEach(function (li) {
      li.hidden = q && li.getAttribute("data-filter").toLowerCase().indexOf(q) === -1;
    });
    document.querySelectorAll(".side details.trk").forEach(function (d) {
      if (q) d.open = !!d.querySelector("li[data-filter]:not([hidden])");
    });
  });

  // keep the current page visible in the sidebar
  var cur = document.querySelector('.side a[aria-current="page"]');
  if (cur && cur.scrollIntoView) cur.scrollIntoView({ block: "center" });

  // copy buttons
  document.querySelectorAll("pre.hl").forEach(function (pre) {
    var b = document.createElement("button");
    b.className = "copy"; b.type = "button"; b.textContent = "Copy";
    b.addEventListener("click", function () {
      var code = pre.querySelector("code");
      navigator.clipboard.writeText(code ? code.innerText : pre.innerText).then(function () {
        b.textContent = "Copied"; setTimeout(function () { b.textContent = "Copy"; }, 1400);
      }, function () { b.textContent = "Press Ctrl+C"; });
    });
    pre.appendChild(b);
  });

  // table of contents scroll-spy
  var links = document.querySelectorAll(".toc a");
  if (links.length && "IntersectionObserver" in window) {
    var map = {};
    links.forEach(function (a) { map[a.getAttribute("href").slice(1)] = a; });
    var obs = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) {
          links.forEach(function (a) { a.classList.remove("active"); });
          var a = map[e.target.id]; if (a) a.classList.add("active");
        }
      });
    }, { rootMargin: "-72px 0px -70% 0px" });
    Object.keys(map).forEach(function (id) { var h = document.getElementById(id); if (h) obs.observe(h); });
  }
})();

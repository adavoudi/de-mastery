(function () {
  var KEY = "de-roadmap-done", done = {};
  try { done = JSON.parse(localStorage.getItem(KEY)) || {}; } catch (e) {}
  var items = document.querySelectorAll(".rm-m"), count = document.getElementById("rm-count"),
      fill = document.getElementById("rm-fill"), reset = document.getElementById("rm-reset");

  function paint() {
    var n = 0;
    items.forEach(function (li) {
      var on = !!done[li.getAttribute("data-id")];
      li.classList.toggle("is-done", on);
      li.querySelector(".rm-done").setAttribute("aria-pressed", on ? "true" : "false");
      if (on) n++;
    });
    count.textContent = n;
    fill.className = "rm-bar-fill p" + Math.round(items.length ? (n / items.length) * 20 : 0) * 5;
    reset.hidden = !n;
  }
  function save() { try { localStorage.setItem(KEY, JSON.stringify(done)); } catch (e) {} }

  items.forEach(function (li) {
    li.querySelector(".rm-done").addEventListener("click", function () {
      var id = li.getAttribute("data-id");
      if (done[id]) delete done[id]; else done[id] = 1;
      save(); paint();
    });
  });
  reset.addEventListener("click", function () {
    if (confirm("Clear all roadmap progress?")) { done = {}; save(); paint(); }
  });
  paint();
})();

(function () {
  var root = document.getElementById("quiz");
  if (!root) return;
  var score = document.getElementById("quiz-score");
  var total = parseInt(root.getAttribute("data-total"), 10) || 0, right = 0, answered = 0;

  root.querySelectorAll("form.q").forEach(function (form) {
    form.addEventListener("submit", function (e) { e.preventDefault(); });
    form.querySelector("[data-check]").addEventListener("click", function () {
      if (form.hasAttribute("data-done")) return;
      var want = form.getAttribute("data-answer").split(",");
      var picked = [].map.call(form.querySelectorAll("input:checked"), function (i) { return i.value; });
      if (!picked.length) return;
      form.setAttribute("data-done", "1");
      var ok = picked.length === want.length && picked.every(function (p) { return want.indexOf(p) !== -1; });
      form.querySelectorAll(".opt").forEach(function (o) {
        var v = o.querySelector("input").value;
        o.querySelector("input").disabled = true;
        if (want.indexOf(v) !== -1) o.classList.add("correct");
        else if (picked.indexOf(v) !== -1) o.classList.add("wrong");
      });
      form.querySelector(".why").hidden = false;
      form.querySelector("[data-check]").hidden = true;
      answered += 1; if (ok) right += 1;
      score.textContent = right + " / " + answered + " correct" + (answered === total ? " · finished" : "");
    });
  });
})();

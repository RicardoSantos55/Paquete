const loginForm = document.querySelector("#login-form");
const loginAlert = document.querySelector("#login-alert");

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...options,
  });
  const payload = await response.json();
  return { response, payload };
}

function showLoginAlert(message, tone = "neutral") {
  loginAlert.textContent = message;
  loginAlert.className = `import-alert ${tone}`;
}

async function checkSession() {
  const { response, payload } = await requestJson("/api/session", {
    method: "GET",
    cache: "no-store",
  });
  if (response.ok && payload.authenticated) {
    window.location.replace("/app.html");
  }
}

async function handleLogin(event) {
  event.preventDefault();
  const formData = new FormData(loginForm);
  const payload = Object.fromEntries(formData.entries());

  showLoginAlert("Validando acceso...", "neutral");
  const { response, payload: data } = await requestJson("/api/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    showLoginAlert(data.error || "No se pudo iniciar sesion.", "danger");
    return;
  }

  window.location.replace("/app.html");
}

loginForm.addEventListener("submit", (event) => {
  handleLogin(event).catch((error) => showLoginAlert(error.message, "danger"));
});

checkSession().catch((error) => showLoginAlert(error.message, "danger"));

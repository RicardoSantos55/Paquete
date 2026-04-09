const elements = {
  sessionUsername: document.querySelector("#session-username"),
  logoutButton: document.querySelector("#logout-button"),
  databaseName: document.querySelector("#database-name"),
  postalCount: document.querySelector("#postal-count"),
  coverageCount: document.querySelector("#coverage-count"),
  routeCount: document.querySelector("#route-count"),
  duplicateCount: document.querySelector("#duplicate-count"),
  sourceName: document.querySelector("#source-name"),
  databaseStatus: document.querySelector("#database-status"),
  importBadge: document.querySelector("#import-badge"),
  importAlert: document.querySelector("#import-alert"),
  selectedFileName: document.querySelector("#selected-file-name"),
  branchSelect: document.querySelector("#branch-select"),
  branchDescription: document.querySelector("#branch-description"),
  resultsBody: document.querySelector("#results-body"),
  resultCount: document.querySelector("#result-count"),
  uploadForm: document.querySelector("#upload-form"),
  manualForm: document.querySelector("#manual-form"),
  fileInput: document.querySelector("#database-file"),
  searchForm: document.querySelector("#search-form"),
  postalCode: document.querySelector("#postal-code"),
};

let branchMap = new Map();

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (character) => {
    const replacements = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;",
    };
    return replacements[character] || character;
  });
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...options,
  });
  const payload = await response.json();
  return { response, payload };
}

async function requireSession() {
  const { response, payload } = await requestJson("/api/session", {
    method: "GET",
    cache: "no-store",
  });
  if (!response.ok || !payload.authenticated) {
    window.location.replace("/");
    return false;
  }
  elements.sessionUsername.textContent = payload.username;
  return true;
}

async function fetchStatus() {
  const { response, payload } = await requestJson("/api/status", {
    method: "GET",
    cache: "no-store",
  });
  if (response.status === 401) {
    window.location.replace("/");
    return;
  }
  if (!response.ok) {
    throw new Error(payload.error || "No se pudo obtener el estado.");
  }
  applyStatus(payload);
}

function applyStatus(payload) {
  branchMap = new Map(payload.branches.map((branch) => [branch.label, branch]));

  elements.databaseName.textContent = payload.databaseLoaded ? payload.databaseName : "Sin base";
  elements.postalCount.textContent = payload.totalPostalCodes.toLocaleString("es-MX");
  elements.coverageCount.textContent = payload.totalCoverageRecords.toLocaleString("es-MX");
  elements.routeCount.textContent = payload.totalRoutes.toLocaleString("es-MX");
  elements.duplicateCount.textContent = payload.duplicatePostalCodes.toLocaleString("es-MX");
  elements.sourceName.textContent = payload.sourceName || "Sin origen registrado";
  elements.databaseStatus.textContent = payload.databaseLoaded
    ? "SQLite lista para consulta"
    : payload.lastError || "Base pendiente";
  elements.importBadge.textContent = payload.databaseLoaded ? "Base lista" : "Pendiente";

  if (!elements.branchSelect.options.length) {
    payload.branches.forEach((branch) => {
      const option = document.createElement("option");
      option.value = branch.label;
      option.textContent = branch.label;
      elements.branchSelect.append(option);
    });
  }

  updateBranchDescription();

  if (payload.duplicatePostalCodes > 0) {
    showImportAlert(
      `Se detectaron ${payload.duplicatePostalCodes.toLocaleString("es-MX")} codigos postales repetidos en la base. ` +
      `Eso representa ${payload.duplicateRows.toLocaleString("es-MX")} filas adicionales con el mismo CP.`,
      "warning"
    );
  } else if (payload.databaseLoaded) {
    showImportAlert("La base de datos esta lista para consulta y no tiene CP repetidos detectados.", "success");
  } else if (payload.lastError) {
    showImportAlert(payload.lastError, "danger");
  } else {
    showImportAlert("Reemplaza la base completa o agrega registros manuales uno por uno.", "neutral");
  }
}

function updateBranchDescription() {
  const branch = branchMap.get(elements.branchSelect.value);
  elements.branchDescription.textContent = branch ? branch.description : "";
}

function showImportAlert(message, tone = "neutral") {
  elements.importAlert.textContent = message;
  elements.importAlert.className = `import-alert ${tone}`;
}

function renderEmptyState(message) {
  elements.resultsBody.innerHTML = `
    <tr>
      <td colspan="10" class="empty-state">${escapeHtml(message)}</td>
    </tr>
  `;
  elements.resultCount.textContent = "0 coincidencias";
}

function renderResults(results) {
  if (!results.length) {
    renderEmptyState("No se encontraron coincidencias para ese codigo postal.");
    return;
  }

  elements.resultsBody.innerHTML = results
    .map((result) => {
      const pillClass = result.distance_km === null
        ? "neutral"
        : result.withinLimit
          ? "success"
          : "danger";
      return `
        <tr>
          <td>${escapeHtml(result.postal_code)}</td>
          <td>${escapeHtml(result.state)}</td>
          <td>${escapeHtml(result.city)}</td>
          <td>${escapeHtml(result.municipality)}</td>
          <td>${escapeHtml(result.neighborhood)}</td>
          <td>${escapeHtml(result.coverage)}</td>
          <td>${escapeHtml(result.destination_branch)}</td>
          <td>${escapeHtml(result.destination_plaza)}</td>
          <td>${escapeHtml(result.distanceLabel)}</td>
          <td><span class="pill ${pillClass}">${escapeHtml(result.limitLabel)}</span></td>
        </tr>
      `;
    })
    .join("");

  const label = results.length === 1 ? "coincidencia" : "coincidencias";
  elements.resultCount.textContent = `${results.length} ${label}`;
}

async function handleLogout() {
  await requestJson("/api/logout", { method: "POST" });
  window.location.replace("/");
}

async function handleUpload(event) {
  event.preventDefault();
  const file = elements.fileInput.files[0];
  if (!file) {
    showImportAlert("Selecciona un archivo .xlsx antes de reemplazar la base.", "danger");
    return;
  }

  const formData = new FormData();
  formData.append("database", file);

  showImportAlert("Importando archivo y reemplazando la base de datos...", "neutral");
  const { response, payload } = await requestJson("/api/import", {
    method: "POST",
    body: formData,
  });

  if (response.status === 401) {
    window.location.replace("/");
    return;
  }
  if (!response.ok) {
    showImportAlert(payload.error || "No se pudo reemplazar la base.", "danger");
    return;
  }

  applyStatus(payload.status);
  renderEmptyState("Los datos fueron importados. Realiza una nueva busqueda.");
  showImportAlert(payload.message, "success");
  elements.selectedFileName.textContent = "Ningun archivo seleccionado";
  elements.uploadForm.reset();
}

async function handleManualSubmit(event) {
  event.preventDefault();
  const formData = new FormData(elements.manualForm);
  const payload = Object.fromEntries(formData.entries());

  showImportAlert("Guardando registro manual y validando si el CP ya existe...", "neutral");
  const { response, payload: data } = await requestJson("/api/manual-entry", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (response.status === 401) {
    window.location.replace("/");
    return;
  }
  if (!response.ok) {
    showImportAlert(data.error || "No se pudo guardar el registro manual.", "danger");
    return;
  }

  applyStatus(data.status);
  elements.manualForm.reset();
  const tone = data.message.includes("ya existia") ? "warning" : "success";
  showImportAlert(data.message, tone);
}

async function handleSearch(event) {
  event.preventDefault();
  const branch = elements.branchSelect.value;
  const postalCode = elements.postalCode.value.trim();

  if (!branch || !postalCode) {
    elements.resultCount.textContent = "CP invalido";
    return;
  }

  const { response, payload } = await requestJson("/api/search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ branch, postalCode }),
  });

  if (response.status === 401) {
    window.location.replace("/");
    return;
  }
  if (!response.ok) {
    renderEmptyState(payload.error || "No se pudo realizar la busqueda.");
    elements.resultCount.textContent = "Error";
    return;
  }

  renderResults(payload.results);
  if (payload.duplicateNotice) {
    const countLabel = payload.results.length === 1 ? "coincidencia" : "coincidencias";
    elements.resultCount.textContent = `${payload.results.length} ${countLabel} | CP repetido`;
  }
}

function handleFileSelection() {
  const file = elements.fileInput.files[0];
  elements.selectedFileName.textContent = file ? file.name : "Ningun archivo seleccionado";
}

elements.logoutButton.addEventListener("click", () => {
  handleLogout().catch(() => window.location.replace("/"));
});
elements.branchSelect.addEventListener("change", updateBranchDescription);
elements.fileInput.addEventListener("change", handleFileSelection);
elements.uploadForm.addEventListener("submit", (event) => {
  handleUpload(event).catch((error) => showImportAlert(error.message, "danger"));
});
elements.manualForm.addEventListener("submit", (event) => {
  handleManualSubmit(event).catch((error) => showImportAlert(error.message, "danger"));
});
elements.searchForm.addEventListener("submit", (event) => {
  handleSearch(event).catch((error) => renderEmptyState(error.message));
});

requireSession()
  .then((authenticated) => {
    if (!authenticated) {
      return;
    }
    return fetchStatus().then(() => renderEmptyState("No hay resultados todavia."));
  })
  .catch(() => window.location.replace("/"));

// api/client.js — all backend calls in one place

async function post(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err =
      (await res.json().catch(() => ({ detail: res.statusText }))) || {};
    throw new Error(err.detail || res.statusText);
  }

  return res.json();
}

export function generateModel(
  userQuery,
  operation,
  existingModel,
  modelType,
  dbEngine
) {
  return post("/workflow/generate", {
    user_query: userQuery,
    operation: operation || "",
    existing_model: existingModel || null,
    model_type: modelType || "both",
    db_engine: dbEngine || "",
  });
}

export function validateAndGenerateSQL(dataModel, operation) {
  return post("/workflow/validate", {
    data_model: dataModel,
    operation: operation,
  });
}

export function approveAndGenerateSQL(dataModel, operation,apply_partitioning=false) {
  return post("/workflow/approve", {
    data_model: dataModel,
    operation: operation,
    apply_partitioning: apply_partitioning, // New field to control partitioning in SQL generation
  });
}

export function applyFeedbackAndGenerateSQL(dataModel, feedback, operation) {
  return post("/workflow/feedback", {
    data_model: dataModel,
    feedback: feedback,
    operation: operation,
  });
}

export function generateERD(sql, title) {
  return post("/workflow/erd", {
    sql: sql,
    title: title || "Entity Relationship Diagram",
  });
}

export function generateERDXML(sql, title) {
  return post("/workflow/erd/xml", {
    sql: sql,
    title: title || "Entity Relationship Diagram",
  });
}

export function generateERDPDM(sql, title) {
  return post("/workflow/erd/pdm", {
    sql: sql,
    title: title || "Physical Data Model",
  });
}

export function generateERDLDM(sql, title) {
  return post("/workflow/erd/ldm", {
    sql: sql,
    title: title || "Logical Data Model",
  });
}

export function generateERDFromModel(dataModel, title) {
  return post("/workflow/erd/from-model", {
    data_model: dataModel,
    title: title || "Entity Relationship Diagram",
  });
}

// #1 — prompt summary for InputForm sidebar
export function getPromptSummary(userQuery, dbEngine, modelType) {
  return post("/workflow/prompt-summary", {
    user_query: userQuery,
    db_engine: dbEngine || "MySQL",
    model_type: modelType || "both",
  });
}
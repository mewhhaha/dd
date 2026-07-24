const output = document.getElementById("output");

async function load() {
  try {
    const response = await fetch("/api/time");
    const payload = await response.json();
    output.textContent = JSON.stringify(payload, null, 2);
  } catch (error) {
    output.textContent = JSON.stringify(
      {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      },
      null,
      2,
    );
  }
}

load();

/**
 * Minimal fetch wrapper used by all API endpoint functions.
 */

export class ApiError extends Error {
  constructor(
    public readonly statusCode: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/**
 * GET a JSON resource from `url`.
 * Throws ApiError on non-2xx responses.
 */
export async function apiFetch<T>(url: string): Promise<T> {
  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    let message = `HTTP ${response.status}`;
    try {
      const json = JSON.parse(text) as { detail?: string; message?: string };
      message = json.detail ?? json.message ?? message;
    } catch {
      // Ignore JSON parse errors — use the generic message.
    }
    throw new ApiError(response.status, message);
  }

  return response.json() as Promise<T>;
}

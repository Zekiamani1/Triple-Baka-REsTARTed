import { injectContextToCarrier } from "./telemetry";
import { receiverConfig } from "./config";
import type { HazardMessage } from "./message";

export interface ReceiverAcknowledgment {
  delivery_id: string;
  receipt_id: string;
  received_at: string;
  duplicate: boolean;
}

export class DeliveryError extends Error {
  constructor(
    message: string,
    readonly retryable: boolean,
    readonly statusCode?: number,
  ) {
    super(message);
    this.name = "DeliveryError";
  }
}

export async function deliverWithRetry(
  event: HazardMessage,
  deliveryId: string,
  heartbeat: () => Promise<void>,
): Promise<ReceiverAcknowledgment> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= receiverConfig.maxAttempts; attempt++) {
    try {
      return await deliverOnce(event, deliveryId);
    } catch (error) {
      lastError = error;
      const deliveryError = normalizeDeliveryError(error);

      if (!deliveryError.retryable || attempt === receiverConfig.maxAttempts) {
        throw new Error(
          `Field Receiver delivery failed after ${attempt} attempt(s): ${deliveryError.message}`,
          { cause: error },
        );
      }

      const exponential = receiverConfig.baseBackoffMs * 2 ** (attempt - 1);
      const backoffMs = Math.min(exponential, receiverConfig.maxBackoffMs);
      await sleepWithHeartbeat(backoffMs, heartbeat);
    }
  }

  throw new Error("Field Receiver delivery failed", { cause: lastError });
}

async function deliverOnce(
  event: HazardMessage,
  deliveryId: string,
): Promise<ReceiverAcknowledgment> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), receiverConfig.timeoutMs);
  const traceHeaders = injectContextToCarrier();

  try {
    const response = await fetch(receiverConfig.url, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${receiverConfig.token}`,
        "idempotency-key": deliveryId,
        "x-delivery-id": deliveryId,
        ...traceHeaders,
      },
      body: JSON.stringify({
        delivery_id: deliveryId,
        event,
      }),
    });

    const responseText = await response.text();
    let responseBody: unknown;
    try {
      responseBody = responseText ? JSON.parse(responseText) : {};
    } catch {
      throw new DeliveryError("Field Receiver returned malformed JSON", response.status >= 500);
    }

    if (!response.ok) {
      const retryable = response.status === 408 || response.status === 425 ||
        response.status === 429 || response.status >= 500;
      throw new DeliveryError(
        `Field Receiver returned HTTP ${response.status}: ${responseText.slice(0, 500)}`,
        retryable,
        response.status,
      );
    }

    if (!isRecord(responseBody)) {
      throw new DeliveryError("Field Receiver acknowledgment is not a JSON object", false, response.status);
    }

    const acknowledgment = responseBody as Partial<ReceiverAcknowledgment>;
    if (
      acknowledgment.delivery_id !== deliveryId ||
      typeof acknowledgment.receipt_id !== "string" ||
      typeof acknowledgment.received_at !== "string"
    ) {
      throw new DeliveryError("Field Receiver acknowledgment is missing required receipt fields", false, response.status);
    }

    return {
      delivery_id: acknowledgment.delivery_id,
      receipt_id: acknowledgment.receipt_id,
      received_at: acknowledgment.received_at,
      duplicate: acknowledgment.duplicate === true,
    };
  } catch (error) {
    if (error instanceof DeliveryError) throw error;
    if (controller.signal.aborted) {
      throw new DeliveryError(`Field Receiver request timed out after ${receiverConfig.timeoutMs}ms`, true);
    }
    throw new DeliveryError(error instanceof Error ? error.message : String(error), true);
  } finally {
    clearTimeout(timeout);
  }
}

async function sleepWithHeartbeat(durationMs: number, heartbeat: () => Promise<void>): Promise<void> {
  const sliceMs = 1000;
  let remaining = durationMs;
  while (remaining > 0) {
    const current = Math.min(sliceMs, remaining);
    await new Promise((resolve) => setTimeout(resolve, current));
    remaining -= current;
    await heartbeat();
  }
}

function normalizeDeliveryError(error: unknown): DeliveryError {
  if (error instanceof DeliveryError) return error;
  return new DeliveryError(error instanceof Error ? error.message : String(error), true);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

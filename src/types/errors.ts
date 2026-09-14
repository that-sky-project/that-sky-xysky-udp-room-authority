export enum ErrorCode {
  OK = 0,
  BAD_REQUEST = 4000,
  UNAUTHORIZED = 4001,
  FORBIDDEN = 4003,
  NOT_FOUND = 4004,
  CONFLICT = 4009,
  NO_CAPACITY = 4029,
  NODE_UNAVAILABLE = 5001,
  NODE_TIMEOUT = 5002,
  PROTOCOL_ERROR = 5003,
  COORDINATION_FAILED = 5004,
  INTERNAL = 5999
}

export class HermesError extends Error {
  public readonly code: ErrorCode;
  public readonly details: unknown;

  public constructor(code: ErrorCode, message: string, details?: unknown) {
    super(message);
    this.name = "HermesError";
    this.code = code;
    this.details = details;
  }
}

export const isHermesError = (error: unknown): error is HermesError => error instanceof HermesError;

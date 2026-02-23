export interface PaginationParams {
  cursor?: string;
  limit: number;
}

export interface PaginatedResult<T> {
  data: T[];
  pagination: {
    next_cursor: string | null;
    has_more: boolean;
    limit: number;
  };
}

export interface ApiResponse<T = unknown> {
  success: true;
  data: T;
  pagination?: {
    next_cursor: string | null;
    has_more: boolean;
    limit: number;
  };
}

export interface ApiError {
  success: false;
  error: {
    code: string;
    message: string;
    details?: Array<{ field: string; message: string }>;
  };
}

export type ApiResult<T = unknown> = ApiResponse<T> | ApiError;

export interface TaskMessage {
  job_id: string;
  payload: string;
  attempt: number;
  priority: number;
  enqueued_at: string;
  timeout_ms: number;
  max_retries: number;
  handler: string;
}

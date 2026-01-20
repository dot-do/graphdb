/**
 * Structured logging interface for GraphDB
 *
 * Provides consistent, structured logging with namespace support,
 * configurable log levels, and JSON or plain text output formats.
 */

/**
 * Available log levels in order of severity
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Logger interface with standard log level methods
 */
export interface Logger {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

/**
 * Configuration options for logging
 */
export interface LogConfig {
  /** Minimum log level to output (default: 'info') */
  level: LogLevel;
  /** Whether to output structured JSON (default: true) */
  structured: boolean;
}

/** Log level priority mapping */
const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/** Global log configuration */
let globalConfig: LogConfig = {
  level: 'info',
  structured: true,
};

/**
 * Configure global logging settings
 *
 * @param config - Partial configuration to merge with existing config
 */
export function configureLogging(config: Partial<LogConfig>): void {
  globalConfig = { ...globalConfig, ...config };
}

/**
 * Get the current logging configuration
 *
 * @returns Current log configuration
 */
export function getLogConfig(): LogConfig {
  return { ...globalConfig };
}

/**
 * Check if a log level should be output given current configuration
 *
 * @param level - The log level to check
 * @returns true if the level should be logged
 */
function shouldLog(level: LogLevel): boolean {
  return LOG_LEVEL_PRIORITY[level] >= LOG_LEVEL_PRIORITY[globalConfig.level];
}

/**
 * Format a log entry for output
 *
 * @param namespace - Logger namespace
 * @param level - Log level
 * @param message - Log message
 * @param fields - Optional structured fields
 * @returns Formatted log string
 */
function formatLog(
  namespace: string,
  level: LogLevel,
  message: string,
  fields?: Record<string, unknown>
): string {
  const timestamp = new Date().toISOString();

  if (globalConfig.structured) {
    const logEntry: Record<string, unknown> = {
      timestamp,
      level,
      namespace,
      message,
    };

    if (fields && Object.keys(fields).length > 0) {
      logEntry['fields'] = fields;
    }

    return JSON.stringify(logEntry);
  }

  // Plain text format
  const fieldsStr =
    fields && Object.keys(fields).length > 0
      ? ` ${Object.entries(fields)
          .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
          .join(' ')}`
      : '';

  return `[${timestamp}] [${level.toUpperCase()}] [${namespace}] ${message}${fieldsStr}`;
}

/**
 * Create a logger with the specified namespace
 *
 * @param namespace - Namespace to identify the logger (e.g., component name)
 * @returns Logger instance
 *
 * @example
 * ```typescript
 * const logger = createLogger('broker');
 * logger.info('Connection established', { clientId: '123' });
 * logger.error('Connection failed', { error: 'timeout' });
 * ```
 */
export function createLogger(namespace: string): Logger {
  return {
    debug(message: string, fields?: Record<string, unknown>): void {
      if (shouldLog('debug')) {
        console.debug(formatLog(namespace, 'debug', message, fields));
      }
    },

    info(message: string, fields?: Record<string, unknown>): void {
      if (shouldLog('info')) {
        console.info(formatLog(namespace, 'info', message, fields));
      }
    },

    warn(message: string, fields?: Record<string, unknown>): void {
      if (shouldLog('warn')) {
        console.warn(formatLog(namespace, 'warn', message, fields));
      }
    },

    error(message: string, fields?: Record<string, unknown>): void {
      if (shouldLog('error')) {
        console.error(formatLog(namespace, 'error', message, fields));
      }
    },
  };
}

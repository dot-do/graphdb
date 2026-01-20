import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createLogger,
  type Logger,
  type LogLevel,
  configureLogging,
  getLogConfig,
} from '../../src/observability/logger';

describe('Logger', () => {
  let consoleDebugSpy: ReturnType<typeof vi.spyOn>;
  let consoleInfoSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleDebugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    consoleInfoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    // Reset config to defaults
    configureLogging({ level: 'info', structured: true });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('createLogger', () => {
    it('should create logger with namespace', () => {
      const logger = createLogger('test-namespace');

      expect(logger).toBeDefined();
      expect(logger.debug).toBeTypeOf('function');
      expect(logger.info).toBeTypeOf('function');
      expect(logger.warn).toBeTypeOf('function');
      expect(logger.error).toBeTypeOf('function');
    });

    it('should include namespace in log output', () => {
      const logger = createLogger('my-component');

      logger.info('test message');

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      expect(logOutput).toContain('my-component');
    });
  });

  describe('log with structured fields', () => {
    it('should log message with structured fields', () => {
      const logger = createLogger('test');

      logger.info('user logged in', { userId: '123', action: 'login' });

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];

      // Should contain the message
      expect(logOutput).toContain('user logged in');
      // Should contain structured fields
      expect(logOutput).toContain('userId');
      expect(logOutput).toContain('123');
      expect(logOutput).toContain('action');
      expect(logOutput).toContain('login');
    });

    it('should log message without fields', () => {
      const logger = createLogger('test');

      logger.info('simple message');

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      expect(logOutput).toContain('simple message');
    });

    it('should handle nested field values', () => {
      const logger = createLogger('test');

      logger.info('complex data', {
        user: { id: '123', name: 'test' },
        count: 42,
      });

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      expect(logOutput).toContain('user');
    });

    it('should include timestamp in structured output', () => {
      const logger = createLogger('test');

      logger.info('timestamped message');

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      // Should contain some form of timestamp indicator
      expect(logOutput).toMatch(/timestamp|time|ts/i);
    });
  });

  describe('log levels', () => {
    it('should support debug level', () => {
      configureLogging({ level: 'debug' });
      const logger = createLogger('test');

      logger.debug('debug message', { detail: 'value' });

      expect(consoleDebugSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleDebugSpy.mock.calls[0][0];
      expect(logOutput).toContain('debug message');
      expect(logOutput).toContain('debug');
    });

    it('should support info level', () => {
      const logger = createLogger('test');

      logger.info('info message');

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      expect(logOutput).toContain('info');
    });

    it('should support warn level', () => {
      const logger = createLogger('test');

      logger.warn('warning message', { code: 'W001' });

      expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleWarnSpy.mock.calls[0][0];
      expect(logOutput).toContain('warning message');
      expect(logOutput).toContain('warn');
    });

    it('should support error level', () => {
      const logger = createLogger('test');

      logger.error('error occurred', { errorCode: 'E500' });

      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleErrorSpy.mock.calls[0][0];
      expect(logOutput).toContain('error occurred');
      expect(logOutput).toContain('error');
    });

    it('should respect log level filtering - debug hidden at info level', () => {
      configureLogging({ level: 'info' });
      const logger = createLogger('test');

      logger.debug('should not appear');
      logger.info('should appear');

      expect(consoleDebugSpy).not.toHaveBeenCalled();
      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
    });

    it('should respect log level filtering - info hidden at warn level', () => {
      configureLogging({ level: 'warn' });
      const logger = createLogger('test');

      logger.debug('should not appear');
      logger.info('should not appear');
      logger.warn('should appear');

      expect(consoleDebugSpy).not.toHaveBeenCalled();
      expect(consoleInfoSpy).not.toHaveBeenCalled();
      expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
    });

    it('should respect log level filtering - only error at error level', () => {
      configureLogging({ level: 'error' });
      const logger = createLogger('test');

      logger.debug('should not appear');
      logger.info('should not appear');
      logger.warn('should not appear');
      logger.error('should appear');

      expect(consoleDebugSpy).not.toHaveBeenCalled();
      expect(consoleInfoSpy).not.toHaveBeenCalled();
      expect(consoleWarnSpy).not.toHaveBeenCalled();
      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('configuration', () => {
    it('should be configurable via configureLogging', () => {
      configureLogging({ level: 'debug' });

      const config = getLogConfig();
      expect(config.level).toBe('debug');
    });

    it('should support structured output format', () => {
      configureLogging({ level: 'info', structured: true });
      const logger = createLogger('test');

      logger.info('structured log');

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      // Structured output should be JSON parseable
      expect(() => JSON.parse(logOutput)).not.toThrow();
    });

    it('should support plain text output format', () => {
      configureLogging({ level: 'info', structured: false });
      const logger = createLogger('test');

      logger.info('plain text log', { key: 'value' });

      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
      const logOutput = consoleInfoSpy.mock.calls[0][0];
      // Plain text should not be JSON (will throw on parse)
      expect(logOutput).toContain('plain text log');
      expect(logOutput).not.toMatch(/^\{.*\}$/);
    });

    it('should preserve config after multiple logger creations', () => {
      configureLogging({ level: 'warn' });

      const logger1 = createLogger('first');
      const logger2 = createLogger('second');

      logger1.info('from first');
      logger2.info('from second');

      // Both should be filtered out at warn level
      expect(consoleInfoSpy).not.toHaveBeenCalled();
    });

    it('should allow runtime reconfiguration', () => {
      const logger = createLogger('test');

      configureLogging({ level: 'error' });
      logger.info('should not appear');
      expect(consoleInfoSpy).not.toHaveBeenCalled();

      configureLogging({ level: 'info' });
      logger.info('should appear now');
      expect(consoleInfoSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('LogLevel type', () => {
    it('should only accept valid log levels', () => {
      // This is a compile-time check, but we verify runtime behavior
      const validLevels: LogLevel[] = ['debug', 'info', 'warn', 'error'];

      validLevels.forEach((level) => {
        expect(() => configureLogging({ level })).not.toThrow();
      });
    });
  });

  describe('Logger interface', () => {
    it('should match Logger interface', () => {
      const logger: Logger = createLogger('test');

      // TypeScript compile-time check that logger has correct shape
      expect(typeof logger.debug).toBe('function');
      expect(typeof logger.info).toBe('function');
      expect(typeof logger.warn).toBe('function');
      expect(typeof logger.error).toBe('function');
    });
  });
});

import { CronExpressionParser } from 'cron-parser';

export function validateCronExpression(expression: string): boolean {
  try {
    CronExpressionParser.parse(expression);
    return true;
  } catch {
    return false;
  }
}

export function getNextRunDate(expression: string, from?: Date): Date {
  const interval = CronExpressionParser.parse(expression, {
    currentDate: from || new Date(),
  });
  return interval.next().toDate();
}

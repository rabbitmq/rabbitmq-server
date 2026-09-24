import { describe, expect, it } from 'vitest'
import { fmtBytes, fmtNum, fmtPercent, fmtRate, fmtRateNum, fmtSiPrefix, severityFor, FD_THRESHOLDS, UNKNOWN } from './numbers'

describe('number formatting', () => {
  it('groups thousands', () => {
    expect(fmtNum(0)).toBe('0')
    expect(fmtNum(1234567)).toBe('1,234,567')
    expect(fmtNum(undefined)).toBe(UNKNOWN)
  })

  it('formats rates with precision that depends on magnitude, like fmt_rate_num', () => {
    expect(fmtRateNum(0)).toBe('0.00')
    expect(fmtRateNum(0.456)).toBe('0.46')
    expect(fmtRateNum(4.56)).toBe('4.6')
    expect(fmtRateNum(4567.8)).toBe('4,568')
    expect(fmtRate(196)).toBe('196/s')
  })

  it('formats bytes with binary prefixes', () => {
    expect(fmtBytes(0)).toBe('0 B')
    expect(fmtBytes(7)).toBe('7 B')
    expect(fmtBytes(1023)).toBe('1023 B')
    expect(fmtBytes(1536)).toBe('1.5 KiB')
    expect(fmtBytes(515 * 1024)).toBe('515 KiB')
    expect(fmtBytes(15461882265)).toBe('14 GiB')
    expect(fmtBytes(null)).toBe(UNKNOWN)
  })

  it('returns 0 for zero, unlike fmt_si_prefix whose zero check never fires', () => {
    expect(fmtSiPrefix(0, 100, false, true)).toBe('0')
    expect(fmtSiPrefix(1500, 1500, false, false)).toBe('1.5 k')
    expect(fmtSiPrefix(1048576, 1048576, false, false)).toBe('1.0 M')
  })

  it('formats ratios as percentages', () => {
    expect(fmtPercent(0.256)).toBe('26%')
    expect(fmtPercent(undefined)).toBe(UNKNOWN)
  })

  it('picks a severity from thresholds, like fmt_color', () => {
    expect(severityFor(0.5, FD_THRESHOLDS)).toBe('green')
    expect(severityFor(0.85, FD_THRESHOLDS)).toBe('yellow')
    expect(severityFor(0.99, FD_THRESHOLDS)).toBe('red')
  })
})

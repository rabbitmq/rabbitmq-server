const TAG_SHORTCUTS: [string, string][] = [
  ['administrator', 'Admin'],
  ['monitoring', 'Monitoring'],
  ['policymaker', 'Policymaker'],
  ['management', 'Management'],
  ['impersonator', 'Impersonator'],
  ['', 'None'],
]

export function TagsInput({ id, value, onChange }: { id: string; value: string; onChange: (value: string) => void }) {
  return (
    <div>
      <input id={id} type="text" value={value} onChange={(e) => onChange(e.target.value)} data-testid="user-tags" />
      <div className="hint">
        Set{' '}
        {TAG_SHORTCUTS.map(([tag, label], i) => (
          <span key={label}>
            {i > 0 ? ' | ' : null}
            <button type="button" className="btn-link" onClick={() => onChange(tag)}>
              {label}
            </button>
          </span>
        ))}
      </div>
    </div>
  )
}

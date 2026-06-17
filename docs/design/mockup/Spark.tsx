// Datatype-font inline chart helper. Renders charts from text via OpenType
// calt/liga ligatures (line {l:..}, bars {b:..}, pie {p:..}). The literal
// {..} expression MUST arrive as a string prop — in JSX a bare {..} would be
// parsed as a JS expression. The ligature glyph is invisible to screen
// readers, so `label` is the required text alternative (aria-label).
export function Spark({
  expr,
  label,
  color,
  size = 28
}: {
  expr: string;
  label: string;
  color?: string;
  size?: number;
}) {
  return <span className="dn-dt" aria-label={label} style={{
    fontSize: size,
    color
  }}>
      {expr}
    </span>;
}
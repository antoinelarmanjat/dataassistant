import math
import html

chart_data = [("Fantasy", 500), ("Science Fiction", 500), ("Crime Fiction", 500)]
total_val = sum(v for _, v in chart_data)

svg_height = max(200, len(chart_data) * 25 + 40)
svg = f'<svg width="600" height="{svg_height}" viewBox="0 0 600 {svg_height}" xmlns="http://www.w3.org/2000/svg">\n'
svg += '  <g transform="translate(150, 150)">\n'

colors = ["#4285F4", "#DB4437", "#F4B400", "#0F9D58", "#AB47BC", "#00ACC1", "#FF7043", "#9E9D24", "#5C6BC0", "#F06292"]
current_pct = 0.0

for i, (label, val) in enumerate(chart_data):
    pct = val / total_val
    if pct == 1.0:
        svg += f'    <circle r="100" cx="0" cy="0" fill="{colors[i % len(colors)]}" />\n'
        break
        
    start_angle = current_pct * 2 * math.pi - math.pi / 2
    end_angle = (current_pct + pct) * 2 * math.pi - math.pi / 2
    
    x1 = 100 * math.cos(start_angle)
    y1 = 100 * math.sin(start_angle)
    x2 = 100 * math.cos(end_angle)
    y2 = 100 * math.sin(end_angle)
    
    large_arc_flag = 1 if pct > 0.5 else 0
    
    path_data = f"M 0 0 L {x1:.2f} {y1:.2f} A 100 100 0 {large_arc_flag} 1 {x2:.2f} {y2:.2f} Z"
    svg += f'    <path d="{path_data}" fill="{colors[i % len(colors)]}" />\n'
    
    current_pct += pct

svg += '  </g>\n'

# Legend
svg += '  <g transform="translate(320, 50)" font-family="sans-serif" font-size="14" fill="currentColor">\n'
for i, (label, val) in enumerate(chart_data):
    color = colors[i % len(colors)]
    y_pos = i * 25
    svg += f'    <rect x="0" y="{y_pos - 12}" width="15" height="15" fill="{color}" />\n'
    svg += f'    <text x="25" y="{y_pos}">{html.escape(label)} ({val})</text>\n'
svg += '  </g>\n</svg>'

print("PIE CHART:")
print(svg)

print("\nBAR CHART:")
svg_b = f'<svg width="600" height="{len(chart_data) * 30 + 20}" xmlns="http://www.w3.org/2000/svg">\n'
svg_b += '  <g font-family="sans-serif" font-size="14" fill="currentColor">\n'

max_val = max((v for _, v in chart_data), default=0.0)
for i, (label, val) in enumerate(chart_data):
    y_pos = i * 30 + 20
    rect_y = i * 30 + 5
    pct = min(1.0, max(0.0, float(val) / max_val)) if max_val > 0 else 0
    bar_width = pct * 300
    
    svg_b += f'    <text x="0" y="{y_pos}">{html.escape(label)}</text>\n'
    svg_b += f'    <rect x="160" y="{rect_y}" width="{bar_width:.1f}" height="20" fill="#4285F4" />\n'
    svg_b += f'    <text x="{160 + bar_width + 10:.1f}" y="{y_pos}">{val}</text>\n'
    
svg_b += '  </g>\n</svg>'
print(svg_b)

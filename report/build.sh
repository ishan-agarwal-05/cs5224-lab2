#!/bin/zsh
# Builds A0284673R.pdf from report.html (inlines analysis/spark_job.py into Appendix A)
cd "$(dirname "$0")"
python3 - <<'PY'
import html
from pathlib import Path
src = Path('report.html').read_text()
code = html.escape(Path('../analysis/spark_job.py').read_text())
Path('.build.html').write_text(src.replace('<!--SPARK-->', code))
PY
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
"$CHROME" --headless --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="A0284673R.pdf" --virtual-time-budget=4000 \
  "file://$PWD/.build.html" 2>/dev/null
rm -f .build.html
echo "built: $PWD/A0284673R.pdf"

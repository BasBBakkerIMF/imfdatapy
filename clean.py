# clean.py
import re

with open("README.md", "r", encoding="utf-8") as f:
    content = f.read()

# Remove style blocks
content = re.sub(r"<style[^>]*>.*?</style>", "", content, flags=re.DOTALL | re.IGNORECASE)

# Optional: collapse extra newlines
content = re.sub(r"\n{3,}", "\n\n", content)

with open("README.md", "w", encoding="utf-8") as f:
    f.write(content)

print("✅ Removed <style> blocks from README.md")


#
# quarto render  README.qmd --to gfm
# python clean.py 

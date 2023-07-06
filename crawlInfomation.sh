INFO='\033[0;32m[INFO]\033[0m '
ERRORStr='\033[0;31m[ERROR]\033[0m '

# 爬取人物信息
echo -e "${INFO}generating nodes and edges excel..."
python3 src/main/python/crawlInfomation.py
if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"
"""
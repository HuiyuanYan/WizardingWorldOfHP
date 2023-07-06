INFO='\033[0;32m[INFO]\033[0m '
ERRORStr='\033[0;31m[ERROR]\033[0m '

# 导出节点、边的excel表
echo -e "${INFO}generating markdown files..."
python3 src/main/python/exportMD.py
if [ $? -ne 0 ]; then
echo -e "${ERROR}failed"
exit 1
fi
echo -e "${INFO}finished"
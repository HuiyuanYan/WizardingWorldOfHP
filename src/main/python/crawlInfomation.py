import requests
import re
from bs4 import BeautifulSoup

#这些词会被删除
filter_words = {"Professor ", "Madam ", "Sir ", "Mr "}

#存放需要爬取的人物名字名单
namePath = 'src/main/resources/name.txt'

#输出目录
outputDir = 'characterInfo/'

headers = {'user-agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.67'}

def getCharacterInfo(character: str):
    # 定义目标网站的 URL
    for words in filter_words:
        character = character.replace(words, '')
    character = character.replace(' ','_')

    url = f"https://harrypotter.fandom.com/{character}"
    # 发送 GET 请求获取网页内容
    response = requests.get(url)

    # 检查请求是否成功
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        quote_paragraph = soup.find('div', class_='quote')
        if quote_paragraph == None:
            return None
        next_paragraphs = []
        next_sibling = quote_paragraph.find_next_sibling()
        while next_sibling and next_sibling.name == 'p':
            next_paragraphs.append(next_sibling.get_text()+'\n')
            next_sibling = next_sibling.find_next_sibling()

    else:

        return None

    infoStr = ''
    for paragraph in next_paragraphs:
        # 使用正则表达式去除引用标记
        text = re.sub(r'\[\d+\]', '', paragraph)
        infoStr += text
    return infoStr


def getCharacterName(nameFilePath):
    name_list = []
    with open(nameFilePath, "r",encoding='utf-8') as file:
        for line in file:
            name_list.append(line.strip())
    return name_list


nameList = getCharacterName(namePath)

succeded_list = []
failed_list = []

for name in nameList:
    print(f"handle info of {name}\n")
    infoStr = getCharacterInfo(name)
    if infoStr == None:
        failed_list.append(name)
        print(f"failed to get infomation of {name}\n")
    else:
        with open(f'{outputDir}{name}.txt', 'w') as file:
            file.write(infoStr)
            file.close()
        succeded_list.append(name)
        print(f"succeded to get infomation of {name}\n")

print(f"successfully crawled the information of {len(succeded_list)} characters: \n")
print(succeded_list)

print(f"failed to crawl the information of {len(failed_list)} characters: \n")
print(failed_list)


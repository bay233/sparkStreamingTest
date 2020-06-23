import xml.dom.minidom

def get_config():
    # 使用minidom解析器打开 XML 文档
    DOMTree = xml.dom.minidom.parse("path.xml")
    collection = DOMTree.documentElement
    Browsers = collection.getElementsByTagName("Browser")
    var = {}
    for Browser in Browsers:
        name = Browser.getElementsByTagName('name')[0].childNodes[0].data
        path = Browser.getElementsByTagName('path')[0].childNodes[0].data
        var[name] = path
    return var

import pymongo

client = pymongo.MongoClient('xxx', xx)
db = client.xxx
col = db.xxx

list_add_word = ['经历', '背景', '简介', '履历', '信息', '研究', '领域']
dict_add_tag = {}
def run_dict(dict_name, dict_add_tag):
    for key, value in dict_name.items():
        # if type(value) != dict:
            # print(key, value[:10])
        # print(type(value))
        for i in list_add_word:
            if i in key and type(value) != dict:
                dict_add_tag[key] = value
        if type(value) == dict:
            run_dict(value, dict_add_tag)
    return dict_add_tag


for num, i in enumerate(col.find()):
    print(num)
    dict_add_tag = {}
    resume_revise = ''
    resume = i['resume']
    tag = i['tag']
    # 检查resume的长度是否达到标准
    if len(resume) < 200:
        dict_add_tag = run_dict(tag, dict_add_tag)
        print(dict_add_tag)

        resume_revise += resume
        if dict_add_tag:
            for i2 in dict_add_tag.values():
                resume_revise += i2
            print(resume_revise)
            print(len(resume_revise))
    else:
        resume_revise = resume

    myquery = {"_id": i['_id']}
    newvalues = {"$set": {"resume_revise": resume_revise}}
    col.update_one(myquery, newvalues)







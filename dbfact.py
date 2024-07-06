import pickle
import os
import json
import uuid


def new(path, columns, primary):
    if not path.endswith("/"):
        path += "/"
    for column in columns:
        os.makedirs(path + column)
    info={"primary" : primary, "columns" : columns}
    file=open(path+"dbinfo", "w")
    json.dump(info, file)
    file.close()

class Db:
    def __init__(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError("Database does not exists")
        if not path.endswith("/"):
            path += "/"
        self.path=path
        file=open(path+"dbinfo")
        info=json.load(file)
        file.close()
        self.primary=info["primary"]
        self.columns=info["columns"]

    def insert(self, datas, indexOnly=None):
        if self.primary not in datas:
            datas[self.primary]=str(uuid.uuid1())

        for column in datas:
            if indexOnly and column not in indexOnly:
                continue
            if column not in self.columns:
                raise KeyError("Unknow column %s"%column)
            fname=self.path+column+"/"+str(datas[column])
            if os.path.exists(fname):
                file=open(fname)
                data=json.load(file)
                file.close()
            else:
                data=[]
            data.append(datas[self.primary])
            file=open(fname, "w")
            json.dump(data, file)
            file.close()
        file=open(self.path+self.primary+"/"+datas[self.primary], "w")
        json.dump(datas, file)
        file.close()

    def select(self, column=None, value=None):
        if not column:
            return list(self.select(self.primary, primary) for primary in os.listdir(self.path+self.primary))
        fname=self.path+column+"/"+str(value)
        if not os.path.exists(fname):
            if not os.path.exists(self.path+column):
                raise KeyError("Column %s does not exists"%column)
            return []
        file=open(fname)
        links=json.load(file)
        file.close()
        if column == self.primary:
            return links
        data=[]
        for link in links:
            data.append(self.select(self.primary, link))
        return data



if __name__=="__main__":
    new("companies", ["id", "company", "website"], primary="id") #create a database called companies with three columns id, company and website and defines id as a primary key
    db=Db("companies") #Load the database called companies
    #Insert companies and websites
    db.insert({"company" : "Google", "website" : "https://google.com"})
    db.insert({"company" : "Apple", "website" : "https://apple.com"})
    db.insert({"company" : "Amazon", "website" : "https://amazon.com"})
    #Select using name of the companies
    results=db.select("company", "Google") #List of dicts like {"id" : "uuid", "company" : "Google", "website" : "https://google.com"}
    print(results[0]["website"])
    results=db.select() #Returns all the companies in the database
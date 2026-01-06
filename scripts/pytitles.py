import sys, newspaper

url = sys.argv[1]

print("Download titles from %s" % (url,))

paper = newspaper.build(url)

for article in paper.articles:
    #try:
    article.download()
    article.parse()
    print(article.title)
   # except:
        #print("Error downloading article")



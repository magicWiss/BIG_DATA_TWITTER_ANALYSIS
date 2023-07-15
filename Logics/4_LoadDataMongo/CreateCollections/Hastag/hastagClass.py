class Hastag:

    def createtopics(self,topics):
        out=dict()
        for k in topics:
            out[k]=1
        return out
    
    def createSentiment(self,sentiment=None):
        if sentiment==None:
            return {"pos":0,"med":0,"neg":0}
            
        out=dict()
        if len(sentiment)==3:
        
            out={"pos":sentiment[0],"med":sentiment[1],"neg":sentiment[2]}
        else:
            out={"pos":0,"med":0,"neg":0}
        return out
    def __init__(self,id,total,users,topics,cluster=None,sentiment=None):

        self.id=id
        self.total=total        #totale di volte che l'hastag è usato
        self.users={users:1}        #user to count (conto il numero di volte che un utetnte ha usato quell'hastag)
        self.topics=self.createtopics(topics)      
        self.clusters={cluster:1}   
        self.sentiment=self.createSentiment(sentiment)
    

    

    
    
    def to_json(self):
        return {
            "id": self.id,
            "total": str(self.total),
            "users": str(self.users),
            "topics":str(self.topics),
            "cluster":str(self.clusters),
            "self.sentimetn":str(self.sentiment)
        }   
    
    def update_users(self,user):
        
        if user not in self.users:
            self.users[user]=0
        self.users[user]+=1
    
    def update_topics(self,topics):
        for k in topics:
            if k not in self.topics:
                self.topics[k]=0
            self.topics[k]+=1

    def update_cluster(self,cluster):
        if cluster not in self.users:
            self.users[cluster]=0
        self.users[cluster]+=1
    
    def update_sentiment(self,sent=None):
        if sent==None:
            return self.sentiment
        if len(sent)==3:
            self.sentiment["pos"]+=sent[0]

            self.sentiment["med"]+=sent[1]
            self.sentiment["neg"]+=sent[2]
    


    def update_Hastag(self,total,users,topics,cluster=None,sentiment=None):
        self.total+=1
        
        self.update_users(users)
        self.update_topics(topics)
        self.update_cluster(cluster)
        self.update_sentiment(sentiment)

    def compute_sentiment(self):
        for k in self.sentiment.keys():
            self.sentiment[k]=self.sentiment[k]/self.total
        return str(self.sentiment)
    def to_row(self):
        #return (hash(self.id),self.id,self.total,str(self.users),str(self.clusters),str(self.topics),str(self.sentiment))
        return (self.id,self.total,str(self.users),str(self.clusters),str(self.topics),self.compute_sentiment())



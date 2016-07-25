import Queue
from pandaharvester.harvesterconfig import harvester_config


from Communicator import Communicator



# method wrapper
class CommunicatorMethod:

    # constructor
    def __init__(self,methodName,pool):
        self.methodName = methodName 
        self.pool = pool


    # method emulation
    def __call__(self,*args,**kwargs):
        try:
            # get connection
            con = self.pool.get()
            # get function
            func = getattr(con,self.methodName)
            # exec
            return apply(func,args,kwargs)
        finally:
            self.pool.put(con)




# connection class
class CommunicatorPool(object):
    
    # constructor
    def __init__(self):
        # install members
        object.__setattr__(self,'pool',None)
        # connection pool
        self.pool = Queue.Queue(harvester_config.pandacon.nConnections)
        for i in range(harvester_config.pandacon.nConnections):
            con = Communicator()
            self.pool.put(con)



    # override __getattribute__
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except:
            pass
        #  method object
        tmpO = CommunicatorMethod(name,self.pool)
        object.__setattr__(self,name,tmpO)
        return tmpO

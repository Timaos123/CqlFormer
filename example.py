from CqlFormer import CqlFormer

if __name__=="__main__":
    myCF=CqlFormer()
    print("How old is Amy:\n{}".format(myCF.getSub(name="Amy").getReturn(sro=[],att=["s.Age"]).run()))
    myCF=CqlFormer()
    print("who knows Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn().run()))
    myCF=CqlFormer()
    print("Amy knows who:\n{}".format(myCF.getSub(name="Amy").getRel(["knows"]).getReturn().run()))
    myCF=CqlFormer()
    print("How many people know Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn(sro=[],agg=[("count","s")]).run()))
    myCF=CqlFormer()
    print("what are the ages of people who know Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn(sro=[],att=["s.name","s.Age"]).run()))
    myCF=CqlFormer()
    print("what is the largest age of people who know Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn(sro=[],agg=[("max","s.Age")]).run()))
    myCF=CqlFormer()
    print("what is the countries/districts of people who know Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn(sro=[],att=["s.country"]).run()))
    myCF=CqlFormer() 
    print("what is the numbers of the countries/districts of people who know Amy:\n{}".format(myCF.getRel(["knows"]).getObj(name="Amy").getReturn(sro=[],att=["s.country"],agg=[("count","s.country")]).run()))
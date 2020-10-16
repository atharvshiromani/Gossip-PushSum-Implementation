#r "nuget: Akka.TestKit"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Configuration"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic

//Akka.Configuration.Coordinated-shutdown.terminate-actor-system = off


let mutable flagmarker = 0
let system = ActorSystem.Create("FSharp")
let mutable numnodes = fsi.CommandLineArgs.[1] |> int32
let topo = fsi.CommandLineArgs.[2] |> string
let algorithm = fsi.CommandLineArgs.[3] |> string
let mutable marker = 0
let mutable diff = 0 |> float
let mutable sumestimate = 0 |> float
let mutable newsumestimate = 0 |> float

//let mutable list1 = new ResizeArray<_>()
let mutable lists = new ResizeArray<_>() 
let mutable listw = new ResizeArray<_>() 


for i in 0..numnodes-1 do
    lists.Add(i+1 |> float)

for i in 0..numnodes-1 do
    listw.Add(1 |> float) 


let mutable list1 = new ResizeArray<_>()
let mutable list2 = new ResizeArray<_>()
let mutable list3 = new List<_>()

let mutable newnum = numnodes |> float
let sq = sqrt (newnum)
if( sq - floor(sq)=0.0) then
    newnum <- newnum
elif( sq - floor(sq) <> 0.5) then
    newnum <- floor(sq) * floor(sq)


//numnodes <- newnum |> int32

for i in 0..numnodes-1 do
    list2.Add(0)

let objrandom = new System.Random()


let timer =
    System.Diagnostics.Stopwatch()



type ProcessorMessage = ProcessJob1 of IActorRef * int



let rec gossipfulltopo (node:IActorRef) =
    
    let abc = new ResizeArray<_>()
    let len = numnodes-1 
    //printfn"%A" node
    //let mutable z = -1
    for i in 0..len do
        if list1.[i] <> node then
            //z <- z + 1
            abc.Add(list1.[i])
    
    //printfn "%A" abc        
    let mutable newnode = 0
    newnode <-  objrandom.Next(0,numnodes-1) 
    //printfn "%i" newnode       
    abc.[newnode] <! ProcessJob1(abc.[newnode], newnode)
    
    let id = list1.FindIndex (fun s -> s = abc.[newnode])
    let mutable di = list2.[id]
    //printfn "current value is:%i" di
    di <- di+1
    list2.[id] <- di
    //printfn "final value: %i" list2.[id]
    //for i in list2 do
    if(list2.[id] = 10) then
        //printfn "final value of actor %i: %i" id list2.[id]
        list3.Add(list1.[id])
        newnode <-  objrandom.Next(0,numnodes-1)
        for i in list3 do
            if abc.[newnode] = i then
                if newnode = 0 then
                    newnode <- objrandom.Next(newnode + 1,numnodes-1)
                elif newnode = numnodes - 2 then
                    newnode <- objrandom.Next(0, newnode)
                else
                    newnode <- newnode+1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = numnodes-2 then
            
            printfn "process completed"
        else    
            gossipfulltopo(abc.[newnode])   
    else
        gossipfulltopo (abc.[newnode])

let rec gossiplinetopo (node:IActorRef) =

    let abc = new ResizeArray<_>()
    let len = numnodes-1 
    
    if( node = list1.[0] ) then
        abc.Add(list1.[1])
    elif( node = list1.[numnodes-1]) then
        abc.Add(list1.[numnodes-2])
    else
        let id1 = list1.FindIndex (fun s -> s = node)
        //printfn"%i" id1
        abc.Add(list1.[id1-1])
        abc.Add(list1.[id1+1])
    
    //printfn "%A" abc
    
    let mutable newnode = 0
    let tail = Seq.last abc
    let tailid = abc.FindIndex (fun s -> s = tail)
    let newtail = tailid + 1
    newnode <-  objrandom.Next(0,newtail) 
    //printfn "%i" newnode      
    abc.[newnode] <! ProcessJob1(abc.[newnode], newnode)
    
    let id = list1.FindIndex (fun s -> s = abc.[newnode])
    let mutable di = list2.[id]
    //printfn "current value is:%i" di
    if(di < 10) then
        di <- di+1
        list2.[id] <- di
    
    //printfn "final value of actor %i : %i" id list2.[id]
    
    
    if(list2.[id] = 10) then
        list3.Add(list1.[id])
        let stop1 = Seq.last list3
        let stopid1 = list3.FindIndex (fun s -> s = stop1)
        //printfn "%i" stopid
        if stopid1 > numnodes-2 then
            printfn "process completed"
        else
            newnode <-  objrandom.Next(0,newtail)
            for i in list3 do
                if abc.[newnode] = i && tailid =1 then
                    if newnode = 0 then
                        newnode <- 1
                    elif newnode = 1 then
                        newnode <- 0
                    
               
            gossiplinetopo(abc.[newnode]) 
    else
        gossiplinetopo (abc.[newnode])

let rec gossip2Dtopo (act:IActorRef) =
    let abc = new List<_>()
    let node = list1.FindIndex (fun s -> s = act)
    if node=0 then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node = int(sqrt(newnum))-1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = numnodes - int(sqrt(newnum)) then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = numnodes - 1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node > 0 && node < int(sqrt(newnum))-1 then
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node + (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif node > numnodes - int(sqrt(newnum))  && node < numnodes-1 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node - (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif (node % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node+(int(sqrt(newnum)) - 1 )])
    elif ((node-3) % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node-(int(sqrt(newnum)) - 1 )])
    else
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
    //abc |> Seq.iteri (fun index item -> printfn "%i: %A" index abc.[index])
    let mutable newnode1 = 0
    let tail = Seq.last abc
    let tailid = abc.FindIndex (fun s -> s = tail)
    let newtail = tailid + 1
    newnode1 <-  objrandom.Next(0,newtail) 
    //printfn "%i" newnode1       
    abc.[newnode1] <! ProcessJob1(abc.[newnode1], newnode1)
    
    let id = list1.FindIndex (fun s -> s = abc.[newnode1])
    let mutable di = list2.[id]
    //printfn "current value is:%i" di
    di <- di+1
    list2.[id] <- di
    //printfn "final value: %i" list2.[id]
    //printfn "final value of actor %i: %i" id list2.[id]
    if(list2.[id] = 10) then
        //printfn "final value of actor %i: %i" id list2.[id]
        list3.Add(list1.[id])
        
        newnode1 <-  objrandom.Next(0,newtail)
        for i in list3 do
            if abc.[newnode1] = i then
                if newnode1 = 0 then
                    newnode1 <- objrandom.Next(newnode1 + 1,newtail)
                elif newnode1 = tailid then
                    newnode1 <- objrandom.Next(0,tailid)
                else
                    newnode1 <- newnode1 + 1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = int(newnum)-2 then
            printfn "process completed"
        else    
            gossip2Dtopo(abc.[newnode1])   
    else
        gossip2Dtopo(abc.[newnode1])
    
let rec gossip2Dimptopo (act:IActorRef) =
    let abc = new List<_>()
    let node = list1.FindIndex (fun s -> s = act)
    if node=0 then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node = int(sqrt(newnum))-1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - int(sqrt(newnum)) then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - 1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node > 0 && node < int(sqrt(newnum))-1 then
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node + (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif node > int(newnum) - int(sqrt(newnum))  && node < int(newnum)-1 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node - (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif (node % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node+(int(sqrt(newnum)) - 1 )])
    elif ((node-3) % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node-(int(sqrt(newnum)) - 1 )])
    else
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
    //abc |> Seq.iteri (fun index item -> printfn "%i: %A" index abc.[index])
    let mutable newnode1 = 0
    let tail = Seq.last abc
    let mutable tailid = abc.FindIndex (fun s -> s = tail)
    

    let randn = objrandom.Next(0,int(newnum))
    for i in 0..tailid-1 do
        if list1.[randn] = (abc.[i]) then 
            randn = objrandom.Next(0,int(newnum))
        else 
            abc.Add(list1.[randn])
            i = tailid + 1

    abc.Remove(list1.[randn])        
    abc.Remove(list1.[randn])          

    let tail1 = Seq.last abc
    tailid = abc.FindIndex (fun s -> s = tail1)
    newnode1 <-  objrandom.Next(0,tailid+1) 
          
    abc.[newnode1] <! ProcessJob1(abc.[newnode1], newnode1)
    
    let id = list1.FindIndex (fun s -> s = abc.[newnode1])
    let mutable di = list2.[id]
    //printfn "current value is:%i" di
    di <- di+1
    list2.[id] <- di
    //printfn "final value: %i" list2.[id]
    if(list2.[id] = 10) then
        //printfn "final value of actor %i: %i" id list2.[id]
        list3.Add(list1.[id])
        newnode1 <-  objrandom.Next(0,tailid+1)
        for i in list3 do
            if abc.[newnode1] = i then
                if newnode1 = 0 then
                    newnode1 <- objrandom.Next(newnode1 + 1,tailid+1)
                elif newnode1 = tailid then
                    newnode1 <- objrandom.Next(0,tailid)
                else
                    newnode1 <- newnode1 + 1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = int(newnum)-2 then
            printfn "process completed"
        else    
            gossip2Dimptopo(abc.[newnode1])   
    else
        gossip2Dimptopo (abc.[newnode1])

let rec pushsumline (node:IActorRef) =
    let id1 = list1.FindIndex (fun s -> s = node)
    let abc = new ResizeArray<_>()
    let len = numnodes-1 
    
    if( node = list1.[0] ) then
        abc.Add(list1.[1])
    elif( node = list1.[numnodes-1]) then
        abc.Add(list1.[numnodes-2])
    else
        let id1 = list1.FindIndex (fun s -> s = node)
        //printfn"%i" id1
        abc.Add(list1.[id1-1])
        abc.Add(list1.[id1+1])
           

    
    let tail = Seq.last abc
    let tailid = abc.FindIndex (fun s -> s = tail)
    let newtail = tailid + 1
    //printfn "newtail : %i" newtail
    let mutable newnode = objrandom.Next(0,newtail) 
    sumestimate <- lists.[newnode]/listw.[newnode] |> float
    //printfn "sumestimate:%f" sumestimate 
    
    //printfn "currents:%f" lists.[id1]
    //printfn "curentw:%f" listw.[id1]

    let no = 2 |> float
    
    //sumestimate <-  lists.[id1]/listw.[id1]
    lists.[id1] <- float(lists.[id1])/no
    listw.[id1] <- float(listw.[id1])/no 
    //printfn "finals:%f" lists.[id1]
    //printfn "finalw:%f" listw.[id1]

    
    
    let mutable valueofS = lists.[id1] 
    let mutable valueofW = listw.[id1] 

    valueofS <- valueofS + (lists.[newnode])
    valueofW <- valueofW + (listw.[newnode])

    lists.[newnode] <- valueofS
    listw.[newnode] <- valueofW
    //printfn "sending s:%f" valueofS 
    //printfn "sending w:%f" valueofW 
        
    newsumestimate <-  lists.[newnode]/listw.[newnode] |> float
    
    //printfn "newsumestimate:%A" newsumestimate
 
    diff <-  sumestimate - newsumestimate |> float
    if diff < 0.0 then
        diff <- diff * (-1.0)
    let maxdif = 0.0000000001 |> float
    if diff > 0.0000000000 && diff <= maxdif  then
        marker <- marker + 1
    else
        marker <- 0
    //printfn "Difference: %A" diff 
    //printfn "Marker Value: %i" marker 

    abc.[newnode] <! ProcessJob1(abc.[newnode],int(valueofW))
    if(marker = 3 ) then
        list3.Add(list1.[id1])
        let stop1 = Seq.last list3
        let stopid1 = list3.FindIndex (fun s -> s = stop1)
        //printfn "%i" stopid
        if stopid1 > numnodes-2 then
            printfn "process completed"
        else
            newnode <-  objrandom.Next(0,newtail)
            for i in list3 do
                if abc.[newnode] = i && tailid =1 then
                    if newnode = 0 then
                        newnode <- 1
                    elif newnode = 1 then
                        newnode <- 0
                    
            
            
               
            pushsumline(abc.[newnode]) 
    
    else
        pushsumline(abc.[newnode])


let rec pushsumfull (node:IActorRef) =
    
    let id1 = list1.FindIndex (fun s -> s = node)
    //printfn "%i" id1
    let abc = new ResizeArray<_>()
    let len = numnodes-1
    //let mutable z = -1
    for i in 0..len do
        if( list1.[i] <> node ) then
            //z <- z + 1
            abc.Add(list1.[i])
    
    //printfn "%A" abc        

    
    let mutable newnode = objrandom.Next(0,numnodes-2) 
    sumestimate <- lists.[newnode]/listw.[newnode] |> float
    //printfn "sumestimate:%f" sumestimate 
    
    //printfn "currents:%f" lists.[id1]
    //printfn "curentw:%f" listw.[id1]

    let no = 2 |> float
    
    //sumestimate <-  lists.[id1]/listw.[id1]
    lists.[id1] <- float(lists.[id1])/no
    listw.[id1] <- float(listw.[id1])/no 
    //printfn "finals:%f" lists.[id1]
    //printfn "finalw:%f" listw.[id1]

    
    
    let mutable valueofS = lists.[id1] 
    let mutable valueofW = listw.[id1] 

    valueofS <- valueofS + (lists.[newnode])
    valueofW <- valueofW + (listw.[newnode])

    lists.[newnode] <- valueofS
    listw.[newnode] <- valueofW
    //printfn "sending s:%f" valueofS 
    //printfn "sending w:%f" valueofW 
        
    newsumestimate <-  lists.[newnode]/listw.[newnode] |> float
    
    //printfn "newsumestimate:%A" newsumestimate
 
    diff <-  sumestimate - newsumestimate |> float
    if diff < 0.0 then
        diff <- diff * (-1.0)
    let maxdif = 0.0000000001 |> float
    if diff > 0.0000000000 && diff <= maxdif  then
        marker <- marker + 1
    else
        marker <- 0
    //printfn "Difference: %A" diff 
    //printfn "Marker Value: %i" marker 

    abc.[newnode] <! ProcessJob1(abc.[newnode],int(valueofW))
    if(marker = 3 ) then
        list3.Add(list1.[newnode])
        newnode <-  objrandom.Next(0,numnodes-2)
        for i in list3 do
            if abc.[newnode] = i then
                if newnode = 0 then
                    newnode <- objrandom.Next(newnode + 1,numnodes-2)
                elif newnode = numnodes - 2 then
                    newnode <- objrandom.Next(0, newnode - 1)
                else
                    newnode <- newnode+1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = numnodes-2 then
            
            printfn "process completed" 
        else    
            pushsumfull(abc.[newnode])  
    else
        pushsumfull(abc.[newnode])

let rec pushsum2D (node1:IActorRef) =
    
    let abc = new List<_>()
    let node = list1.FindIndex (fun s -> s = node1)
    //printfn "%i" node
    if node=0 then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node = int(sqrt(newnum))-1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - int(sqrt(newnum)) then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - 1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int((newnum) - (sqrt(newnum)))])
    elif node > 0 && node < int(sqrt(newnum))-1 then
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node + (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif node > int(newnum) - int(sqrt(newnum))  && node < int(newnum)- 1 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node - (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif (node % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node+(int(sqrt(newnum)) - 1 )])
    elif ((node-3) % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node-(int(sqrt(newnum)) - 1 )])
    else
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
    
    //printfn "---------------"        

    
    
    let tail = Seq.last abc
    let tailid = abc.FindIndex (fun s -> s = tail)
    let newtail = tailid + 1 
    //printfn "newtail is:%i" newtail
    let mutable newnode = objrandom.Next(0,newtail) 
    //printfn "%i" newnode
    sumestimate <- lists.[newnode]/listw.[newnode] |> float
    //printfn "sumestimate:%f" sumestimate 
    
    //printfn "currents:%f" lists.[node]
    //printfn "curentw:%f" listw.[node]

    let no = 2 |> float
    
    //sumestimate <-  lists.[id1]/listw.[id1]
    lists.[node] <- float(lists.[node])/no
    listw.[node] <- float(listw.[node])/no 
    //printfn "finals:%f" lists.[id1]
    //printfn "finalw:%f" listw.[id1]

    
    
    let mutable valueofS = lists.[node] 
    let mutable valueofW = listw.[node] 

    valueofS <- valueofS + (lists.[newnode])
    valueofW <- valueofW + (listw.[newnode])

    lists.[newnode] <- valueofS
    listw.[newnode] <- valueofW
    //printfn "sending s:%f" valueofS 
    //printfn "sending w:%f" valueofW 
        
    newsumestimate <-  lists.[newnode]/listw.[newnode] |> float
    
    //printfn "newsumestimate:%A" newsumestimate
    
    diff <-  sumestimate - newsumestimate |> float
    if diff < 0.0 then
        diff <- diff * (-1.0)
    let maxdif = 0.0000000001 |> float
    if diff > 0.0000000000 && diff <= maxdif  then
        marker <- marker + 1
    else
        marker <- 0
    //printfn "Difference: %A" diff 
    //printfn "Marker Value:%i" marker 

    abc.[newnode] <! ProcessJob1(abc.[newnode],int(valueofW))
    if(marker = 3 ) then
        list3.Add(list1.[node])
        newnode <-  objrandom.Next(0,tailid)
        //let newtail = tailid - 1
        for i in list3 do
            if abc.[newnode] = i then
                if newnode = 0 then
                    newnode <- objrandom.Next(newnode + 1,tailid)
                
                elif newnode = tailid then
                    newnode <- objrandom.Next(0,tailid)
                else
                    newnode <- newnode + 1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = int(newnum)-2 then
            printfn "process completed" 
        else    
            pushsum2D(abc.[newnode])
        
    elif marker < 3 then
        pushsum2D(abc.[newnode])

let rec pushsumimp2D (node1:IActorRef) =
    
    let abc = new List<_>()
    let node = list1.FindIndex (fun s -> s = node1)
    //printfn "%i" node
    if node=0 then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int(newnum - (sqrt(newnum)))])
    elif node = int(sqrt(newnum))-1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - int(sqrt(newnum)) then 
        abc.Add(list1.[node+1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(newnum)-1])
        abc.Add(list1.[0])
    elif node = int(newnum) - 1 then 
        abc.Add(list1.[node-1]) 
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[int(sqrt(newnum))-1])
        abc.Add(list1.[int((newnum) - (sqrt(newnum)))])
    elif node > 0 && node < int(sqrt(newnum))-1 then
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node + (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif node > int(newnum) - int(sqrt(newnum))  && node < int(newnum)- 1 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node - (int(sqrt(newnum)) * (int(sqrt(newnum)) - 1 ))])
    elif (node % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node+1])
        abc.Add(list1.[node+(int(sqrt(newnum)) - 1 )])
    elif ((node-3) % int(sqrt(newnum))) = 0 then
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node-(int(sqrt(newnum)) - 1 )])
    else
        abc.Add(list1.[node-int(sqrt(newnum))])
        abc.Add(list1.[node+int(sqrt(newnum))])
        abc.Add(list1.[node-1])
        abc.Add(list1.[node+1])
    
    //printfn "---------------"        

    
    
    let tail = Seq.last abc
    let tailid = abc.FindIndex (fun s -> s = tail)
    let newtail = tailid + 1 
    //printfn "newtail is:%i" newtail
    let randn = objrandom.Next(0,int(newnum)-1)
    for i in 0..tailid-1 do
        if list1.[randn] = (abc.[i]) then 
            randn = objrandom.Next(0,int(newnum)-1)
        else 
            abc.Add(list1.[randn])
            i = tailid + 1
    let mutable newnode = objrandom.Next(0,newtail) 
    //printfn "%i" newnode
    sumestimate <- lists.[newnode]/listw.[newnode] |> float
    //printfn "sumestimate:%f" sumestimate 
    
    //printfn "currents:%f" lists.[node]
    //printfn "curentw:%f" listw.[node]

    let no = 2 |> float
    
    //sumestimate <-  lists.[id1]/listw.[id1]
    lists.[node] <- float(lists.[node])/no
    listw.[node] <- float(listw.[node])/no 
    //printfn "finals:%f" lists.[id1]
    //printfn "finalw:%f" listw.[id1]

    
    
    let mutable valueofS = lists.[node] 
    let mutable valueofW = listw.[node] 

    valueofS <- valueofS + (lists.[newnode])
    valueofW <- valueofW + (listw.[newnode])

    lists.[newnode] <- valueofS
    listw.[newnode] <- valueofW
    //printfn "sending s:%f" valueofS 
    //printfn "sending w:%f" valueofW 
        
    newsumestimate <-  lists.[newnode]/listw.[newnode] |> float
    
    //printfn "newsumestimate:%A" newsumestimate
    
    diff <-  sumestimate - newsumestimate |> float
    if diff < 0.0 then
        diff <- diff * (-1.0)
    let maxdif = 0.0000000001 |> float
    if diff > 0.0000000000 && diff <= maxdif  then
       marker <- marker + 1
    else
        marker <- 0
    //printfn "Difference: %A" diff 
    //printfn "Marker Value:%i" marker 

    abc.[newnode] <! ProcessJob1(abc.[newnode],int(valueofW))
    if(marker = 3 ) then
        list3.Add(list1.[node])
        newnode <-  objrandom.Next(0,tailid)
        //let newtail = tailid - 1
        for i in list3 do
            if abc.[newnode] = i then
                if newnode = 0 then
                    newnode <- objrandom.Next(newnode + 1,tailid)
                
                elif newnode = tailid then
                    newnode <- objrandom.Next(0,tailid)
                else
                    newnode <- newnode + 1
        let stop = Seq.last list3
        let stopid = list3.FindIndex (fun s -> s = stop)
        //printfn "%i" stopid
        if stopid = int(newnum)-2 then
            printfn "process completed"
        else    
            pushsumimp2D(abc.[newnode])
        
    elif marker < 3 then
        pushsumimp2D(abc.[newnode])
        
let topology (mailbox: Actor<_>) =

    let rec loop() = actor {
        let! ProcessJob1(x,y) = mailbox.Receive()
        
        let mutable value = 0
        value <- y + 1

               
    }
    loop()

for i in 1..numnodes do
    list1.Add(spawn system (string i) topology)


//let chosen = list1.[(3*((numnodes)/4))] //only the index number
//printfn "%A" chosen 
let chosen = list1.[objrandom.Next(0,numnodes-2)]
let chosen1 = list1.[objrandom.Next(0,int(newnum)-2)]

//list3.Add(list1.[chosen])

let main () = 
    if algorithm.Equals("gossip") then
        if topo.Equals("full") then
            gossipfulltopo(chosen)
        elif topo.Equals("line") then
            gossiplinetopo(chosen)
        elif topo.Equals("2D") then
            gossip2Dtopo(chosen1)
        elif topo.Equals("imp2D") then
            gossip2Dimptopo(chosen1)
    elif algorithm.Equals("pushsum") then
        if topo.Equals("full") then
            pushsumfull(chosen)
        elif topo.Equals("line") then
            pushsumline(chosen)
        elif topo.Equals("2D") then
            pushsum2D(chosen1)
        elif topo.Equals("imp2D") then
            pushsumimp2D(chosen1)
        

timer.Start()
main()
timer.Stop()

//printfn "%A" list2

printfn "Elapsed Milliseconds: %i" timer.ElapsedMilliseconds
system.Terminate()
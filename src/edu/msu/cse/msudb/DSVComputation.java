package edu.msu.cse.msudb;

import java.util.ArrayList;
import java.util.Map;

public class DSVComputation implements Runnable {
	//It computes the LST
	public void run(){ 
		//System.out.println("DSV computation");
		ArrayList<Long> minVV = new ArrayList<Long>();
		
		for (int i=0 ; i < MServer.numberOfDcs ; i++)
		{
			minVV.add(MServer.vv.get(i));
		}
		//now we compare the lst with lst of the children. Note that if the node is a leaf, this for will be skiped.
		for (Map.Entry<Integer, ArrayList<Long>> entry : MServer.childrenVVs.entrySet())
		{
			for (int i=0 ; i < MServer.numberOfDcs ; i++)
			{
				if (minVV.get(i) > entry.getValue().get(i)) minVV.set(i, entry.getValue().get(i));
			}
		}
		//Now, if the node is root it sends DSV wave to the tree, otherwise it sends its lst to its parent. 
		if (MServer.parent == MServer.pn)
		{
			//System.out.println("Inside DSVComputation, root. Size of children= " + MServer.children.size());
			String dsvMessage = "DSV" + MServer.mainDelimiter;
			for (int i=0 ; i < MServer.numberOfDcs ; i++)
			{
				MServer.DSV.set(i, minVV.get(i));
				//System.out.println("Inside DSVComputation, dcnumber= " + i + " minVV= " + minVV.get(i));
				dsvMessage += minVV.get(i) + MServer.interDepItemDelimiter;
			}
			for (int child: MServer.children)
			{
				MServer.sendToPartition(MServer.dcn, child, dsvMessage);
			}
		}
		else
		{
			String vvMessage = "VV" + MServer.mainDelimiter + MServer.pn + MServer.mainDelimiter; 
			for (int i=0 ; i < MServer.numberOfDcs ; i++)
			{
				vvMessage += minVV.get(i) +  MServer.interDepItemDelimiter ;
			}
			MServer.sendToPartition(MServer.dcn, MServer.parent, vvMessage);
		}
		
	}

}

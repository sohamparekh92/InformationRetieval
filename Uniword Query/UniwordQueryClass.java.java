import java.util.*;
import java.io.*;
public class UniQ2
{
	public static void main(String[] args) 
	{
		HashMap<String, ArrayList<Integer>> hmap = new HashMap<>();
 		Scanner sc;
		Scanner sc2;
		Scanner pa;
		StringBuilder sb;
		String tr;
		FileReader xf;
		try 
		{
			xf = new FileReader("Uniwordoutput.txt");
			BufferedReader br = new BufferedReader(xf);
			String bufferstring = br.readLine();
			while(bufferstring!=null)
			{
				ArrayList<Integer> superlist = new ArrayList<>();
				String[] ar = bufferstring.split("[|]");
				tr = ar[1];
				sb = new StringBuilder(tr);
				sb = sb.deleteCharAt(0);
				tr = sb.toString();
				tr = tr.replaceAll(" ","").trim().replaceAll(",", " ").replaceAll("\\[","").replaceAll("\\]", "");
				sc = new Scanner(tr);
				for(int i=0;sc.hasNextInt();i++)
				superlist.add(sc.nextInt());
				hmap.put(ar[0], superlist);
				bufferstring = br.readLine();
			}
			
			
			ArrayList<Integer> baap = new ArrayList<>();
			ArrayList<Integer> beta = new ArrayList<>();
			
			System.out.println(baap);
			sc2 = new Scanner(System.in);
			Scanner scanint = new Scanner(System.in);
			int tn;
			System.out.println(" how many terms?:  ");
			tn = scanint.nextInt();
			System.out.println(" Use operators: AND, OR, NOT ");
			
			System.out.println("Enter a query");
			String query = sc2.nextLine();
			pa = new Scanner(query);
		
			String [] term = new String [tn];
			String [] op = new String [tn-1];
			for(int i=0;i<tn;i++)
			{
			term[i]=pa.next();
			if(pa.hasNext())
			op[i]=pa.next();
			else break;
			}
			baap=hmap.get(term[0]);
			for(int i=0;i<tn-1;i++)
			{
				beta = hmap.get(term[i+1]);
				if(op[i].equals("AND"))
				{
					baap = andFunc(baap,beta);	
				}
				else if(op[i].equals("OR"))
				{
					baap = orFunc(baap, beta);
					
				}
				else if(op[i].equals("NOT"))
				{
					baap = Negate(baap, beta);
				}
					
			}
			System.out.println("Result of the query:"+baap.toString());

			br.close();
			scanint.close();
		}	
		
		
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}	
	}
	
	
	public static ArrayList<Integer> Negate(ArrayList<Integer> listOne,ArrayList<Integer> listTwo)
    {
        ArrayList<Integer> ans = new ArrayList<Integer>();
        ArrayList<Integer> ins = new ArrayList<Integer>();
        int a=0;//b=0;
        
        ans=andFunc(listOne,listTwo);
        ins=listOne;
        while(a<ans.size())
        {
           if(ins.contains(ans.get(a)))
            ins.remove(ans.get(a));
         
         a+=1;
        }
        Collections.sort(ins);
        return ins;
}

	public static ArrayList<Integer> andFunc(ArrayList<Integer> listOne,ArrayList<Integer> listTwo)
    {
        ArrayList<Integer> chotu = new ArrayList<Integer>();
        int a=0,b=0;//,count=0;   
        while(a<listOne.size()&&b<listTwo.size())
        {
           if(listOne.get(a).equals(listTwo.get(b)))
           {  
               chotu.add(listOne.get(a));
               a++;
               b++;
           }
           else
           {
        	   if(listOne.get(a)< listTwo.get(b))
                   a+=1;
               else
                   b+=1;
           }
          }
        return chotu;
    }
    public static ArrayList<Integer> orFunc(ArrayList<Integer> listOne,ArrayList<Integer> p2)
    {
        ArrayList<Integer> x = new ArrayList<Integer>();
        
        x=andFunc(listOne,p2);
        
        int a=0,j=0;
        while(a<listOne.size())
        {
           if(!x.contains(listOne.get(a))) 
               x.add(listOne.get(a));
           a++;
        }
        while(j<p2.size())
           {
               if(!x.contains(p2.get(j)))
               x.add(p2.get(j));
               j++;
               System.out.println("ins"+x);
           }
           Collections.sort(x);
           return x;
    	}
	}
	
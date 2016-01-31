import java.util.*;
import java.io.*;
public class BiwordQueryClass
{
	public static void main(String[] args) 
	{
		HashMap<String, ArrayList<Integer>> hmap = new HashMap<>();
 		Scanner sc;
 		int termcount;
		Scanner sc2;
		Scanner pa;
		StringBuilder sb;
		String tr;
		FileReader xf;
		try 
		{
			xf = new FileReader("Biwordoutput.txt");
			BufferedReader br = new BufferedReader(xf);
			String bufferstring = br.readLine();
			while(bufferstring!=null)
			{
				ArrayList<Integer> superlist = new ArrayList<>();
				String[] splitstr = bufferstring.split("[|]");
				tr = splitstr[1];
				sb = new StringBuilder(tr);
				sb = sb.deleteCharAt(0);
				tr = sb.toString();
				tr = tr.replaceAll(" ","").trim().replaceAll(",", " ").replaceAll("\\[","").replaceAll("\\]", "");
				sc = new Scanner(tr);
				for(int i=0;sc.hasNextInt();i++)
				superlist.add(sc.nextInt());
				hmap.put(splitstr[0], superlist);
				bufferstring = br.readLine();
			}
			
			ArrayList<Integer> alpha = new ArrayList<>();
			ArrayList<Integer> beta = new ArrayList<>();
			sc2 = new Scanner(System.in);
			Scanner scanint = new Scanner(System.in);
			System.out.println(" how many terms?:  ");
			termcount = scanint.nextInt();
			System.out.println(" Use operators: AND, OR, NOT; and space to separate query-terms");
			System.out.println("Enter a query");
			String query = sc2.nextLine();
			pa = new Scanner(query);
			String [] term = new String [termcount];
			String [] operator = new String [termcount-1];
			for(int i=0;i<termcount;i++)
			{
				term[i]=pa.next()+" "+pa.next();
				if(pa.hasNext())
				operator[i]=pa.next();
				else break;
			}
			alpha=hmap.get(term[0]);
			for(int i=0;i<termcount-1;i++)
			{
				beta = hmap.get(term[i+1]);
				
				if(operator[i].equals("AND"))
				alpha = andFunc(alpha,beta);	
				
				else if(operator[i].equals("OR"))
				alpha = orFunc(alpha, beta);
				
				else if(operator[i].equals("NOT"))
				alpha = notFunc(alpha, beta);
			}
			System.out.println("Query returns DocIDs:"+alpha.toString());
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
	
	
	public static ArrayList<Integer> notFunc(ArrayList<Integer> listOne,ArrayList<Integer> listTwo)
    {
		int a=0;
		ArrayList<Integer> temp = new ArrayList<Integer>();
        ArrayList<Integer> ans = new ArrayList<Integer>();
        //b=0;
        ans=andFunc(listOne,listTwo);
        temp=listOne;
        while(a<ans.size())
        {
           if(temp.contains(ans.get(a)))
           temp.remove(ans.get(a));
           a+=1;
        }
        Collections.sort(temp);
        return temp;
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
               a+=1;
               b+=1;
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
    public static ArrayList<Integer> orFunc(ArrayList<Integer> listOne,ArrayList<Integer> listTwo)
    {
    	int a=0,b=0;
        ArrayList<Integer> x = new ArrayList<Integer>();
        x=andFunc(listOne,listTwo);
        while(a<listOne.size())
        {
           if(!x.contains(listOne.get(a))) 
               x.add(listOne.get(a));
           a+=1;
        }
        while(b<listTwo.size())
           {
               if(!x.contains(listTwo.get(b)))
               x.add(listTwo.get(b));
               b++;
               System.out.println("ins"+x);
           }
           Collections.sort(x);
           return x;
    	}
	}
	
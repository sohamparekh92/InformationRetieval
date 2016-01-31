import java.io.FileNotFoundException;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Parserizor {
	
public static void main(String args[])
{
	DocumentBuilderFactory Genfactory = DocumentBuilderFactory.newInstance();
	int imain =0;
	int strno = 2286 ;
	for(imain = 0; imain < 900000 ; imain++  )
	{
		
		String filename = strno+"newsML.xml";
		strno++;
		try {
			DocumentBuilder builderOne = Genfactory.newDocumentBuilder();
			Document docFile = builderOne.parse("newscratch/soham/xml/data2/"+filename);
			CreateFile fx = new CreateFile();
			fx.openF();
			System.out.println("Currently Parsing: "+filename);
			NodeList rootNL = docFile.getElementsByTagName("newsitem");
			Node rootNode = rootNL.item(0);
			Element rootElement = (Element)rootNode;
			String printx = rootElement.getAttribute("itemid");
			fx.PrintRec(printx);
			fx.PrintRec(rootElement.getAttribute("id"));
			fx.PrintRec(rootElement.getAttribute("date"));
			fx.PrintRec(rootElement.getAttribute("xml:lang"));
			Node titleNode = rootElement.getElementsByTagName("title").item(0);
			Element titleElement = (Element)titleNode;
			fx.PrintRec(titleElement.getTextContent());
			Node headlineNode = rootElement.getElementsByTagName("headline").item(0);
			Element headlineElement = (Element)headlineNode;
			fx.PrintRec(headlineElement.getTextContent());
			Node textNode = rootElement.getElementsByTagName("text").item(0);
			Element textElement = (Element)textNode;
			NodeList pList = textElement.getElementsByTagName("p");
			for(int i=0; i<pList.getLength();i++)
			{
				Node pNode = pList.item(i);
				Element pElement = (Element)pNode;
				fx.PrintRec(pElement.getTextContent());
			}
			Node copyrightNode = rootElement.getElementsByTagName("copyright").item(0);
			Element copyrightElement = (Element)copyrightNode;
			fx.PrintRec(copyrightElement.getTextContent());
			Node metadataNode = rootElement.getElementsByTagName("metadata").item(0);
			Element metadataElement = (Element)metadataNode;
			NodeList codesList = metadataElement.getElementsByTagName("codes");
			for(int i=0;i<codesList.getLength();i++)
			{
				Node codesNode = codesList.item(i);
				Element codesElement =  (Element)codesNode;
				fx.PrintRec(codesElement.getAttribute("class"));
				NodeList codeList = codesElement.getElementsByTagName("code");
				for(int j=0;j<codeList.getLength();j++)
				{
					Node codeNode = codeList.item(j);
					Element codeElement = (Element)codeNode;		
					fx.PrintRec(codeElement.getAttribute("code"));		
					Node  editdetailNode = codeElement.getElementsByTagName("editdetail").item(0);		
					Element editdetailElement = (Element)editdetailNode;
					fx.PrintRec(editdetailElement.getAttribute("attribution"));
					fx.PrintRec(editdetailElement.getAttribute("action"));
					fx.PrintRec(editdetailElement.getAttribute("date"));
				}
			}
			NodeList dcList = metadataElement.getElementsByTagName("dc");
			for(int j=0;j<dcList.getLength();j++)
			{
				Node dcNode = dcList.item(j);
				Element dcElement = (Element) dcNode;
				fx.PrintRec(dcElement.getAttribute("element"));
				fx.PrintRec(dcElement.getAttribute("value"));
			}
			fx.closeFile();
		}
		catch (Exception e)
		{
			//System.out.println("Exception");
			imain++;
		}
		
	}
}
}

class CreateFile
{
	private Formatter x;
	static Integer txtno=1;
	
	public void openF()
	{
		try
		{
			String temp = txtno.toString();
			x = new Formatter("/home/sohamap/xmloutput/"+temp);
			txtno++;
		}
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}
	}
	public void PrintRec(String g)
	{
		x.format("%s%s", g,"\n");
	}
	public void closeFile()
	{
		x.close();
	}
}

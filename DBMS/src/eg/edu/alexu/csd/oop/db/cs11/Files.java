package eg.edu.alexu.csd.oop.db.cs11;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

import java.io.IOException;

import javafx.util.Pair;
import java.util.ArrayList;

public class Files {

	private CurrentDatabase current = CurrentDatabase.getInstance();

	public void createXML(String tableName) {
		File file = new File(current.getPath() + System.getProperty("file.separator") + tableName + ".xml");
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder builder = dbf.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element name = doc.createElement(tableName);
			doc.appendChild(name);
			DOMSource source = new DOMSource(doc);
			Result result = new StreamResult(file);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			try {
				Transformer transformer = transformerFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				try {
					transformer.transform(source, result);
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (TransformerConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createXSD(String tableName, HashMap<String, String> data) {
		File file = new File(current.getPath() + System.getProperty("file.separator") + tableName + ".xsd");
		String NSPREFIX = "xs:";
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {

			DocumentBuilder builder = dbf.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element schemaRoot = doc.createElementNS(XMLConstants.W3C_XML_SCHEMA_NS_URI, NSPREFIX + "schema");
			doc.appendChild(schemaRoot);
			Element names = doc.createElement(NSPREFIX + "element");
			names.setAttribute("name", tableName);
			Element complex = doc.createElement("complexType");
			names.appendChild(complex);
			schemaRoot.appendChild(names);
			Element sequence = doc.createElement("sequence");
			complex.appendChild(sequence);
			for (Entry<String, String> entry : data.entrySet()) {
				Element column = doc.createElement(NSPREFIX + "element");
				column.setAttribute("name", entry.getKey());
				column.setAttribute("type", NSPREFIX + entry.getValue());
				sequence.appendChild(column);
			}
			DOMSource source = new DOMSource(doc);
			Result result = new StreamResult(file);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer;
			try {
				transformer = transformerFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				try {
					transformer.transform(source, result);
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (TransformerConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int deleteFromTable(String table, ArrayList<Pair<String, Pair<String, String>>> conditions, String op)
			throws ParserConfigurationException, SAXException, IOException {
		File file = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
		int changedRows = 0;
		boolean currentRow = true;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = dbf.newDocumentBuilder();
		Document doc = builder.parse(current.getPath() + System.getProperty("file.separator") + table + ".xml");
		Node root = doc.getElementsByTagName(table).item(0);
		HashMap<String, String> xsdMap = readXSD(table);
		ArrayList<String> allColumns = new ArrayList<>();
		for (String col : xsdMap.keySet()) {
			allColumns.add(col);
		}
		if (op.equals("or")) {
			for (int r = 0; r < conditions.size(); r++) {
				Pair<String, Pair<String, String>> condition = conditions.get(r);
				for (int j = 0; j < allColumns.size(); j++) {
					if (condition.getKey().equalsIgnoreCase(allColumns.get(j))) {
						NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(j));
						for (int i = 0; i < conditionColumn.getLength(); i++) {
							Element element = (Element) conditionColumn.item(i);
							if (checkCondition(condition, element)) {
								Node parent = element.getParentNode();
								root.removeChild(parent);
								changedRows++;
								i--;
							}
							DOMSource source = new DOMSource(doc);
							Result result = new StreamResult(file);
							TransformerFactory transformerFactory = TransformerFactory.newInstance();
							Transformer transformer;
							try {
								transformer = transformerFactory.newTransformer();
								transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
								transformer.setOutputProperty(OutputKeys.INDENT, "yes");
								try {
									transformer.transform(source, result);
								} catch (TransformerException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							} catch (TransformerConfigurationException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

						}
					}
				}

			}
		} else if (op.equals("and")) {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			Pair<String, Pair<String, String>> condition2 = conditions.get(1);
			NodeList condition2Column = null, conditionColumn = null;
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(j))) {
					conditionColumn = doc.getElementsByTagName(allColumns.get(j));
				}
			}
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition2.getKey().equalsIgnoreCase(allColumns.get(j))) {
					condition2Column = doc.getElementsByTagName(allColumns.get(j));
				}
			}
			for (int i = 0; i < conditionColumn.getLength(); i++) {
				Element element = (Element) conditionColumn.item(i);
				if (checkCondition(condition, element)) {
					for (int j = 0; j < condition2Column.getLength(); j++) {
						Element element2 = (Element) condition2Column.item(i);
						if (checkCondition(condition2, element2)) {
							Node parent = element.getParentNode();
							root.removeChild(parent);
							changedRows++;
						}
					}
				}
				DOMSource source = new DOMSource(doc);
				Result result = new StreamResult(file);
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer;
				try {
					transformer = transformerFactory.newTransformer();
					transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
					transformer.setOutputProperty(OutputKeys.INDENT, "yes");
					try {
						transformer.transform(source, result);
					} catch (TransformerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (TransformerConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		} else if (op.equals("not")) {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(j))) {
					NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(j));
					for (int i = 0; i < conditionColumn.getLength(); i++) {
						Element element = (Element) conditionColumn.item(i);
						if (!checkCondition(condition, element)) {
							Node parent = element.getParentNode();
							root.removeChild(parent);
							changedRows++;
							i--;
						}
						DOMSource source = new DOMSource(doc);
						Result result = new StreamResult(file);
						TransformerFactory transformerFactory = TransformerFactory.newInstance();
						Transformer transformer;
						try {
							transformer = transformerFactory.newTransformer();
							transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
							transformer.setOutputProperty(OutputKeys.INDENT, "yes");
							try {
								transformer.transform(source, result);
							} catch (TransformerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} catch (TransformerConfigurationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
			}

		} else if (op.equals("all")) {
			HashMap<String, String> map = readXSD(table);
			ArrayList<String> columns = new ArrayList<>();
			for (String col : map.keySet()) {
				columns.add(col);
			}
			for (int j = 0; j < columns.size(); j++) {
				NodeList conditionColumn = doc.getElementsByTagName(columns.get(j));
				Element element = (Element) conditionColumn.item(0);
				Node parent = element.getParentNode();
				root.removeChild(parent);
				changedRows++;
				DOMSource source = new DOMSource(doc);
				Result result = new StreamResult(file);
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer;
				try {
					transformer = transformerFactory.newTransformer();
					transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
					transformer.setOutputProperty(OutputKeys.INDENT, "yes");
					try {
						transformer.transform(source, result);
					} catch (TransformerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (TransformerConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(j))) {
					NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(j));
					for (int i = 0; i < conditionColumn.getLength(); i++) {
						Element element = (Element) conditionColumn.item(i);
						if (checkCondition(condition, element)) {
							Node parent = element.getParentNode();
							root.removeChild(parent);
							changedRows++;
							i--;
						}
						DOMSource source = new DOMSource(doc);
						Result result = new StreamResult(file);
						TransformerFactory transformerFactory = TransformerFactory.newInstance();
						Transformer transformer;
						try {
							transformer = transformerFactory.newTransformer();
							transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
							transformer.setOutputProperty(OutputKeys.INDENT, "yes");
							try {
								transformer.transform(source, result);
							} catch (TransformerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} catch (TransformerConfigurationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
			}
		}
		return changedRows;
	}

	public int insertIntoTable(String table, HashMap<String, String> columns) {
		ArrayList<String> allColumns = new ArrayList<>();
		HashMap<String,String> xsdMap=readXSD(table);
		for(Entry<String,String> entry:xsdMap.entrySet()) {
			allColumns.add(entry.getKey());
		}
		File file = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
		ArrayList<String> givenColumns = new ArrayList<>();
		HashMap<String, Boolean> valid = new HashMap<>();
		boolean exists = false;
		for (Entry<String, String> entry : columns.entrySet()) {
			givenColumns.add(entry.getKey());
		}
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder builder = dbf.newDocumentBuilder();
			Document doc = builder.parse(current.getPath() + System.getProperty("file.separator") + table + ".xml");
			Element root = (Element) doc.getElementsByTagName(table).item(0);
			Element row = doc.createElement("row");
			for (int i = 0; i < allColumns.size(); i++) {
				exists = false;
				for (int j = 0; j < givenColumns.size(); j++) {
					if (allColumns.get(i).equalsIgnoreCase(givenColumns.get(j))) {
						columns.put(allColumns.get(i), columns.get(givenColumns.get(j)));
						exists = true;
					}
				}
				valid.put(allColumns.get(i), exists);
			}
			for (Entry<String, Boolean> entry : valid.entrySet()) {
				if (entry.getValue()) {
					Element col = doc.createElement(entry.getKey());
					Text Value = doc.createTextNode(columns.get(entry.getKey()));
					col.appendChild(Value);
					row.appendChild(col);
				} else {
					Element col = doc.createElement(entry.getKey());
					Text Value = doc.createTextNode("null");
					col.appendChild(Value);
					row.appendChild(col);
				}
			}
			root.appendChild(row);
			DOMSource source = new DOMSource(doc);
			Result result = new StreamResult(file);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer;
			try {
				transformer = transformerFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				try {
					transformer.transform(source, result);
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (TransformerConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return 1;
	}

	public HashMap<String, String> readXSD(String table) {
		HashMap<String, String> columns = new HashMap<>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		//File file = new File(current.getPath() + System.getProperty("file.separator") + table + ".xsd");
		try {
			DocumentBuilder builder = dbf.newDocumentBuilder();
			try {
				Document doc = builder.parse(current.getPath() + System.getProperty("file.separator") + table + ".xsd");
				NodeList allColumns = doc.getElementsByTagName("xs:element");
				for (int i = 0; i < allColumns.getLength(); i++) {
					Node col = allColumns.item(i);
					if (col.getNodeType() == Node.ELEMENT_NODE && col.getAttributes().getLength() > 1) {
						NamedNodeMap attributes = col.getAttributes();
						String colName = attributes.item(0).getNodeValue();
						String colType = attributes.item(1).getNodeValue();
						columns.put(colName, colType);
					}
				}
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return columns;
	}

	public Object[][] selectFromTable(String table, ArrayList<String> columns,
			ConditionFilter filter) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		ArrayList<ArrayList<Object>> array = new ArrayList<>();
		ArrayList<ArrayList<Object>> filterArray = new ArrayList<>();
		Object[][] tableArray = null;
		HashMap<String, String> xsdMap = readXSD(table);
		ArrayList<String> allColumns=new ArrayList<>();
		for(Entry<String,String> entry:xsdMap.entrySet()) {
			allColumns.add(entry.getKey());
		}
		try {
			builder = dbf.newDocumentBuilder();
			Document doc;
			try {
				doc = builder.parse(current.getPath() + System.getProperty("file.separator") + table + ".xml");
				for (int i = 0; i < allColumns.size(); i++) {
							String type = xsdMap.get(allColumns.get(i));
							NodeList columnElements = doc.getElementsByTagName(allColumns.get(i));
							ArrayList<Object> sub = new ArrayList<>();
							for (int j = 0; j < columnElements.getLength(); j++) {
								Node element = columnElements.item(j);
								if (element.getNodeType() == Node.ELEMENT_NODE) {
									String elementValue = element.getTextContent();
									if (type.equals("xs:varchar")) {
										sub.add(elementValue);
									} else if (type.equals("xs:int")) {
										sub.add(Integer.parseInt(elementValue));
									}
								}
							}
							array.add(sub);	
				}
				if (filter != null) {
					filterArray = filter.satisfyCondition(array);
					int rows = filterArray.get(0).size();
					tableArray = new Object[rows][columns.size()];
					for (int i = 0; i < columns.size(); i++) {
						for (int j = 0; j < rows; j++) {
							tableArray[j][i] = filterArray.get(i).get(j);
						}
					}
				} else {
					int rows = array.get(0).size();
					tableArray = new Object[rows][columns.size()];
					for (int i = 0; i < columns.size(); i++) {
						for (int j = 0; j < rows; j++) {
							tableArray[j][i] = array.get(i).get(j);
						}
					}
				}
				return tableArray;
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableArray;
	}

	public int updateTable(String table, HashMap<String, String> columns,
			ArrayList<Pair<String, Pair<String, String>>> conditions, String op)
			throws ParserConfigurationException, SAXException, IOException {
		File file = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
		int changedRows = 0;
		boolean currentRow = true;
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = dbf.newDocumentBuilder();
		Document doc = builder.parse(current.getPath() + System.getProperty("file.separator") + table + ".xml");
		HashMap<String, String> xsdMap = readXSD(table);
		ArrayList<String> allColumns = new ArrayList<>();
		for (String col : xsdMap.keySet()) {
			allColumns.add(col);
		}
		if (op.equals("or")) {
			for (int r = 0; r < conditions.size(); r++) {
				Pair<String, Pair<String, String>> condition = conditions.get(r);
				for (int h = 0; h < allColumns.size(); h++) {
					if (condition.getKey().equalsIgnoreCase(allColumns.get(h))) {
						NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(h));
						for (int i = 0; i < conditionColumn.getLength(); i++) {
							currentRow = true;
							Element element = (Element) conditionColumn.item(i);
							if (checkCondition(condition, element)) {
								Node parent = element.getParentNode();
								NodeList children = parent.getChildNodes();
								for (int j = 0; j < children.getLength(); j++) {
									Node node = children.item(j);
									if (node.getNodeType() == Node.ELEMENT_NODE) {
										for (int z = 0; z < node.getChildNodes().getLength(); z++) {
											Node textNode = node.getChildNodes().item(z);
											for (Entry<String, String> entry : columns.entrySet()) {
												if (entry.getKey()
														.equalsIgnoreCase(textNode.getParentNode().getNodeName())) {
													textNode.setTextContent(columns.get(entry.getKey()));
													if (currentRow) {
														changedRows++;
													}
													currentRow = false;
												}
											}
										}
									}

								}
							}
						}
					}

					DOMSource source = new DOMSource(doc);
					Result result = new StreamResult(file);
					TransformerFactory transformerFactory = TransformerFactory.newInstance();
					Transformer transformer;
					try {
						transformer = transformerFactory.newTransformer();
						transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
						transformer.setOutputProperty(OutputKeys.INDENT, "yes");
						try {
							transformer.transform(source, result);
						} catch (TransformerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (TransformerConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} else if (op.equals("and")) {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			Pair<String, Pair<String, String>> condition2 = conditions.get(1);
			NodeList condition2Column = null, conditionColumn = null;
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(j))) {
					conditionColumn = doc.getElementsByTagName(allColumns.get(j));
				}
			}
			for (int j = 0; j < allColumns.size(); j++) {
				if (condition2.getKey().equalsIgnoreCase(allColumns.get(j))) {
					condition2Column = doc.getElementsByTagName(allColumns.get(j));
				}
			}
			for (int i = 0; i < conditionColumn.getLength(); i++) {
				currentRow = true;
				Element element = (Element) conditionColumn.item(i);
				if (checkCondition(condition, element)) {
					for (int h = 0; h < condition2Column.getLength(); h++) {
						Element element2 = (Element) condition2Column.item(i);
						if (checkCondition(condition2, element2)) {
							Node parent = element.getParentNode();
							NodeList children = parent.getChildNodes();
							for (int j = 0; j < children.getLength(); j++) {
								Node node = children.item(j);
								if (node.getNodeType() == Node.ELEMENT_NODE) {
									for (int z = 0; z < node.getChildNodes().getLength(); z++) {
										Node textNode = node.getChildNodes().item(z);
										for (Entry<String, String> entry : columns.entrySet()) {
											if (entry.getKey()
													.equalsIgnoreCase(textNode.getParentNode().getNodeName())) {
												textNode.setTextContent(columns.get(entry.getKey()));
												if (currentRow) {
													changedRows++;
												}
												currentRow = false;
											}
										}
									}
								}

							}
						}
					}
				}
			}
			DOMSource source = new DOMSource(doc);
			Result result = new StreamResult(file);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer;
			try {
				transformer = transformerFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				try {
					transformer.transform(source, result);
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (TransformerConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (op.equals("not")) {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			for (int h = 0; h < allColumns.size(); h++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(h))) {
					NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(h));
					for (int i = 0; i < conditionColumn.getLength(); i++) {
						currentRow = true;
						Element element = (Element) conditionColumn.item(i);
						if (!checkCondition(condition, element)) {
							Node parent = element.getParentNode();
							NodeList children = parent.getChildNodes();
							for (int j = 0; j < children.getLength(); j++) {
								Node node = children.item(j);
								if (node.getNodeType() == Node.ELEMENT_NODE) {
									for (int z = 0; z < node.getChildNodes().getLength(); z++) {
										Node textNode = node.getChildNodes().item(z);
										for (Entry<String, String> entry : columns.entrySet()) {
											if (entry.getKey()
													.equalsIgnoreCase(textNode.getParentNode().getNodeName())) {
												textNode.setTextContent(columns.get(entry.getKey()));
												if (currentRow) {
													changedRows++;
												}
												currentRow = false;
											}
										}
									}
								}

							}
						}
					}

					DOMSource source = new DOMSource(doc);
					Result result = new StreamResult(file);
					TransformerFactory transformerFactory = TransformerFactory.newInstance();
					Transformer transformer;
					try {
						transformer = transformerFactory.newTransformer();
						transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
						transformer.setOutputProperty(OutputKeys.INDENT, "yes");
						try {
							transformer.transform(source, result);
						} catch (TransformerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (TransformerConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		} else if (op.equals("all")) {
			Set<String> colls = readXSD(table).keySet();
			ArrayList<String> cols = new ArrayList<>();
			for (String entry : colls) {
				cols.add(entry);
			}
			for (int i = 0; i < cols.size(); i++) {
				currentRow = true;
				Element element = (Element) doc.getElementsByTagName(cols.get(i)).item(i);
				if (element != null) {	
					Node parent = element.getParentNode();
					NodeList children = parent.getChildNodes();
					for (int j = 0; j < children.getLength(); j++) {		
						Node node = children.item(j);
						
						if (node.getNodeType() == Node.ELEMENT_NODE) {
							//System.out.println(node.getNodeName());
							for (int z = 0; z < node.getChildNodes().getLength(); z++) {
								Node textNode = node.getChildNodes().item(z);
								for (Entry<String, String> entry : columns.entrySet()) {
									if (entry.getKey().equalsIgnoreCase(textNode.getParentNode().getNodeName())) {
										textNode.setTextContent(columns.get(entry.getKey()));
										//System.out.println((textNode.getParentNode().getNodeName()));
										//System.out.println(textNode.getTextContent());
										if (currentRow) {
											changedRows++;
										}
										currentRow = false;
									}
								}
							}
						}

					}
				}
			}

			DOMSource source = new DOMSource(doc);
			Result result = new StreamResult(file);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer;
			try {
				transformer = transformerFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				try {
					transformer.transform(source, result);
				} catch (TransformerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (TransformerConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			Pair<String, Pair<String, String>> condition = conditions.get(0);
			for (int h = 0; h < allColumns.size(); h++) {
				if (condition.getKey().equalsIgnoreCase(allColumns.get(h))) {
					NodeList conditionColumn = doc.getElementsByTagName(allColumns.get(h));
					for (int i = 0; i < conditionColumn.getLength(); i++) {
						currentRow = true;
						Element element = (Element) conditionColumn.item(i);
						if (checkCondition(condition, element)) {
							Node parent = element.getParentNode();
							NodeList children = parent.getChildNodes();
							for (int j = 0; j < children.getLength(); j++) {
								Node node = children.item(j);
								if (node.getNodeType() == Node.ELEMENT_NODE) {
									for (int z = 0; z < node.getChildNodes().getLength(); z++) {
										Node textNode = node.getChildNodes().item(z);
										// System.out.println(textNode.getParentNode().getNodeName());
										// System.out.println(textNode.getTextContent());
										for (Entry<String, String> entry : columns.entrySet()) {
											if (entry.getKey()
													.equalsIgnoreCase(textNode.getParentNode().getNodeName())) {
												textNode.setTextContent(columns.get(entry.getKey()));
												if (currentRow) {
													changedRows++;
												}
												currentRow = false;
											}
										}
									}
								}

							}
						}
					}
				}

				DOMSource source = new DOMSource(doc);
				Result result = new StreamResult(file);
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer;
				try {
					transformer = transformerFactory.newTransformer();
					transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
					transformer.setOutputProperty(OutputKeys.INDENT, "yes");
					try {
						transformer.transform(source, result);
					} catch (TransformerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (TransformerConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return changedRows;
	}

	public boolean checkCondition(Pair<String, Pair<String, String>> condition, Element element) {
		String conditionValue = condition.getValue().getKey();
		String conditionOperator = condition.getValue().getValue();
		// System.out.println(conditionValue);
		// System.out.println(element.getTextContent());
		switch (conditionOperator) {
		case "==":
			if ((element.getTextContent()).equalsIgnoreCase(conditionValue)) {
				return true;
			}
			break;
		case "=":
			if (element.getTextContent().equals(conditionValue)) {
				return true;
			}
			break;
		case ">":
			if (Integer.parseInt(element.getTextContent()) > Integer.parseInt(conditionValue)) {
				return true;
			}
			break;
		case "<":
			if (Integer.parseInt(element.getTextContent()) > Integer.parseInt(conditionValue)) {
				return true;
			}
			break;
		}
		return false;
	}

}

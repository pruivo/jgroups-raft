package org.jgroups.raft.util;

import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Iterates over all concrete Protocol classes and creates tables with Protocol's properties.
 * These tables are in turn then merged into asciidoc.
 * 
 * Iterates over unsupported and experimental classes and creates tables listing those classes.
 * These tables are in turn then merged into asciidoc.
 *
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * 
 */
public class PropsToAsciidoc {


    public static void main(String[] args) {
        if (args.length != 1) {
            help();
            System.err.println("args[0]=" + args[0]);
            return;
        }
        String prot_file = args[0];
        String temp_file = prot_file + ".tmp";

        try {
            // first copy protocols.adoc file into protocols.adoc.xml
            File f = new File(temp_file);
            copy(new FileReader(new File(prot_file)), new FileWriter(f));
            String s = fileToString(f);

            ClassLoader cl=Thread.currentThread().getContextClassLoader();
            Set<Class<?>> classes=Util.findClassesAssignableFrom("org.jgroups.protocols.raft", Protocol.class, cl);
            // classes.addAll(Util.findClassesAssignableFrom("org.jgroups.protocols.pbcast",Protocol.class));
            Properties props = new Properties();
            for (Class<?> clazz : classes)
                convertProtocolToAsciidocTable(props,clazz);
            String result = Util.substituteVariable(s, props);
            FileWriter fw = new FileWriter(f, false);
            fw.write(result);
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("PropsToAsciidoc <path to protocols.adoc file>");
    }


    protected static void convertUnsupportedToAsciidocTable(Properties props, List<Class<?>> clazzes, String title)
      throws ParserConfigurationException, TransformerException {
        List<String[]> rows=new ArrayList<>(clazzes.size() +1);
        rows.add(new String[]{"Package","Class"}); // add column titles first
        for(Class<?> clazz: clazzes)
            rows.add(new String[]{clazz.getPackage().getName(), clazz.getSimpleName()});

        String tmp=createAsciidocTable(rows, title, "[align=\"left\",width=\"50%\",options=\"header\"]");

        // do we have more than one property (superclass Protocol has only one property (stats))
        if (clazzes.size() > 1) {
            props.put(title, tmp);
        }
    }

    /** Creates an AsciiDoc table of the elements in rows. The first tuple needs to be the column names, the
     * rest the contents */
    protected static String createAsciidocTable(List<String[]> rows, String title, String header)
      throws ParserConfigurationException, TransformerException {
        StringBuilder sb=new StringBuilder(".").append(title).append("\n");
        sb.append(header).append("\n");
        sb.append("|=================\n");
        for(String[] row: rows) {
            for(String el: row)
                sb.append("|").append(el);
            sb.append("\n");
        }
        sb.append("|=================\n");
        return sb.toString();
    }


    private static void convertProtocolToAsciidocTable(Properties props, Class<?> clazz) throws Exception {
        boolean isUnsupported = clazz.isAnnotationPresent(Unsupported.class);
        if (isUnsupported)
            return;

        Map<String, String> nameToDescription = new TreeMap<>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Property.class)) {
                String property = field.getName();
                Property annotation = field.getAnnotation(Property.class);
                String desc = annotation.description();
                nameToDescription.put(property, desc);
            }
        }

        // iterate methods
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)) {

                Property annotation = method.getAnnotation(Property.class);
                String desc = annotation.description();

                if(desc == null || desc.isEmpty())
                    desc="n/a";

                String name = annotation.name();
                if (name.length() < 1) {
                    name = Util.methodNameToAttributeName(method.getName());
                }
                nameToDescription.put(name, desc);
            }
        }


        // do we have more than one property (superclass Protocol has only one property (stats))
        if (nameToDescription.isEmpty())
            return;

        List<String[]> rows=new ArrayList<>(nameToDescription.size() +1);
        rows.add(new String[]{"Name", "Description"});
        for(Map.Entry<String,String> entry: nameToDescription.entrySet())
            rows.add(new String[]{entry.getKey(), entry.getValue()});

        String tmp=createAsciidocTable(rows, clazz.getSimpleName(), "[align=\"left\",width=\"90%\",cols=\"2,10\",options=\"header\"]");
        props.put(clazz.getSimpleName(), tmp);
    }


    private static String fileToString(File f) throws Exception {
        StringWriter output = new StringWriter();
        FileReader input = new FileReader(f);
        char[] buffer = new char[8 * 1024];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toString();
    }

    public static int copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[8 * 1024];
        int count = 0;
        int n = 0;
        try {
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
        } finally {
            output.flush();
            output.close();
        }
        return count;
    }
}

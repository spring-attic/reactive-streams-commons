package rsc.util;

import java.io.File;
import java.net.URL;
import java.nio.file.*;
import java.util.*;

import org.reactivestreams.Publisher;

import rsc.flow.*;
import rsc.processor.UnicastProcessor;
import rsc.publisher.Px;

/**
 * Builds a HTML matrix of fusion between operator pairs.
 */
public final class FusionMatrix {
    
    public static void scanFiles(List<Class<?>> classes, File dir, String pkg) throws Exception {
        File[] fs = dir.listFiles();
        
        for (File f : fs) {
            String name = f.getName();
            if (name.endsWith(".class")) {
                name = name.substring(0, name.length() - 6);
                Class<?> clazz = null;
                try {
                    clazz = Class.forName(pkg + "." + name);
                } catch (Exception ex) {
                    // ignoring
                }
                
                if (clazz != null && Publisher.class.isAssignableFrom(clazz)) {
                    // filter for fusion supporting classes
                    if (clazz.isAnnotationPresent(FusionSupport.class)) {
                        classes.add(clazz);
                    }
                }
            }
        }
    }
    
    static String trim(String name) {
        if (name.startsWith("Publisher")) {
            return name.substring(9);
        }
        return name;
    }
    
    static boolean hasInner(FusionMode[] inner) {
        return inner.length != 0 && inner[0] != FusionMode.NONE && inner[0] != FusionMode.NOT_APPLICABLE;
    }
    
    static void createRelation(StringBuilder b, 
            Class<?> first, FusionMode[] firstModes, Class<?> second, FusionMode[] secondModes) {
        EnumSet<FusionMode> ef = EnumSet.noneOf(FusionMode.class);
        ef.addAll(Arrays.asList(firstModes));
        
        EnumSet<FusionMode> es = EnumSet.noneOf(FusionMode.class);
        es.addAll(Arrays.asList(secondModes));
        
        ef.retainAll(es);
        ef.remove(FusionMode.NONE);
        
        if (ef.isEmpty()) {
            b.append("'>Unfuseable");
        } else {
            b.append("' bgcolor='#FFCC80'>");
            int i = 0;
            for (FusionMode fm : ef) {
                b.append("        ")
                .append(fm.toString().substring(0, 1))
                .append(fm.toString().substring(1).toLowerCase());
                if (i != ef.size() - 1) {
                    b.append("<br/>");
                }
                b.append("\r\n");
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        
        StringBuilder b = new StringBuilder();

        {
            URL u = Px.class.getResource("Px.class");
            File f = new File(u.toURI()).getParentFile();
            scanFiles(classes, f, "rsc.publisher");
        }
        {
            URL u = UnicastProcessor.class.getResource("UnicastProcessor.class");
            File f = new File(u.toURI()).getParentFile();
            scanFiles(classes, f, "rsc.processor");
        }
        
        Collections.sort(classes, (a, c) -> trim(a.getSimpleName()).compareToIgnoreCase(trim(c.getSimpleName())));
        
        b.append("<html><head><title>Reactive-Streams-Commons Fusion Matrix</title></head>\r\n")
        .append("<body><center><h1>Rsc Fusion Matrix</h1></center>\r\n");
        
        b.append("<table border='1' style='border-collapse:collapse;'><thead><td><b>First \\ Second</b></td>\r\n");
        for (Class<?> clazz : classes) {
            FusionSupport ff = clazz.getAnnotation(FusionSupport.class);

            b.append("    <td width='125'><b>").append(trim(clazz.getSimpleName())).append("</b></td>\r\n");
            
            if (hasInner(ff.innerInput())) {
                b.append("    <td width='125' bgColor='#CCFFCC'><b>").append(trim(clazz.getSimpleName())).append(" - inner</b></td>\r\n");
            }
        }
        b.append("</thead><tbody>\r\n");
        
        for (Class<?> first : classes) {
            
            FusionSupport ff = first.getAnnotation(FusionSupport.class);
            
            b.append("<tr><td><b>").append(trim(first.getSimpleName())).append("</b></td>\r\n");
            
            for (Class<?> second : classes) {
                b.append("    <td title='")
                .append(trim(first.getSimpleName()))
                .append(" -&gt; ")
                .append(trim(second.getSimpleName()))
                .append("' ");
                
                FusionSupport fs = second.getAnnotation(FusionSupport.class);
                createRelation(b, first, ff.output(), second, fs.input());
                
                b.append("</td>\r\n");
                
                if (hasInner(fs.innerInput())) {
                    b.append("    <td title='")
                    .append(trim(first.getSimpleName()))
                    .append(" -&gt; ")
                    .append(trim(second.getSimpleName()))
                    .append(" - inner' ");
                    
                    createRelation(b, first, ff.output(), second, fs.innerInput());
                    
                    b.append("</td>\r\n");
                }
            }
            b.append("</tr>\r\n");

            if (hasInner(ff.innerOutput())) {
                b.append("<tr><td bgColor='#CCFFCC'><b>").append(trim(first.getSimpleName())).append(" - inner</td></td>\r\n");

                for (Class<?> second : classes) {
                    b.append("    <td title='")
                    .append(trim(first.getSimpleName()))
                    .append(" -&gt; ")
                    .append(trim(second.getSimpleName()))
                    .append("' ");
                    
                    FusionSupport fs = second.getAnnotation(FusionSupport.class);
                    createRelation(b, first, ff.innerOutput(), second, fs.input());
                    
                    b.append("</td>\r\n");
                    
                    if (hasInner(fs.innerInput())) {
                        b.append("    <td title='")
                        .append(trim(first.getSimpleName()))
                        .append(" - inner -&gt; ")
                        .append(trim(second.getSimpleName()))
                        .append(" - inner' ");
                        
                        createRelation(b, first, ff.innerOutput(), second, fs.innerInput());

                        b.append("</td>\r\n");
                    }
                }

                b.append("</tr\r\n");
            }
        }
        
        b.append("</tbody></table>\r\n");
        
        b.append("</body></html>\r\n");
        
        Files.write(Paths.get("fusion-matrix.html"), Collections.singletonList(b.toString()));
    }
}

package com.deshaw.pjrmi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Arrays;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

/**
 * How we dynamically compile and inject compiled classes from a string.
 */
public class SourceInjector
{
    /**
     * A {@code SimpleJavaFileObject} represented as a {@code String}.
     */
    private static class JavaStringObject
        extends SimpleJavaFileObject
    {
        /**
         * The source code in the file object.
         */
        private final String mySource;

        /**
         * CTOR.
         */
        protected JavaStringObject(final String name, final String source)
        {
            // Convert the name to a valid URI
            super(URI.create("string:///"                +
                             name.replaceAll("\\.", "/") +
                             Kind.SOURCE.extension),
                  Kind.SOURCE);

            mySource = source;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CharSequence getCharContent(final boolean ignoreEncodingErrors)
        {
            // No need to read any files; we already have the source code
            return mySource;
        }
    }

    /**
     * A {@link SimpleJavaFileObject} represented as a {@link ByteArrayOutputStream}.
     */
    private static class JavaByteObject
        extends SimpleJavaFileObject
    {
        /**
         * The byte stream.
         */
        private ByteArrayOutputStream myOutputStream;

        /**
         * CTOR.
         */
        protected JavaByteObject(final String name)
            throws URISyntaxException
        {
            // Convert the name to a valid URI
            super(URI.create("bytes:///" + name.replaceAll("\\.", "/")),
                  Kind.CLASS);

            myOutputStream = new ByteArrayOutputStream();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public OutputStream openOutputStream()
        {
            return myOutputStream;
        }

        /**
         * Get a copy of the {@code byte[]} from the underlying stream.
         */
        public byte[] getBytes()
        {
            return myOutputStream.toByteArray();
        }
    }

    /**
     * Create a custom {@link ForwardingJavaFileManager} so that it
     * recognizes the given {@code byteObject} as a file.
     */
    private static JavaFileManager
        createFileManager(final StandardJavaFileManager fileManager,
                          final JavaByteObject          byteObject)
    {
        return new ForwardingJavaFileManager<StandardJavaFileManager>(fileManager)
        {
            /**
             * {@inheritDoc}
             */
            @Override
            public JavaFileObject
                getJavaFileForOutput(final Location            location,
                                     final String              className,
                                     final JavaFileObject.Kind kind,
                                     final FileObject          sibling)
            {
                return byteObject;
            }
        };
    }

    /**
     * Create a custom ClassLoader so that it can find the {@code byteObject}
     * as the definition for a class.
     */
    private static ClassLoader createClassLoader(final JavaByteObject byteObject)
    {
        return new ClassLoader()
        {
            /**
             * {@inheritDoc}
             */
            @Override
            public Class<?> findClass(final String className)
            {
                // This replaces the need to search for the class
                final byte[] bytes = byteObject.getBytes();

                return defineClass(className, bytes, 0, bytes.length);
            }
        };
    }

    /**
     * Compiles and injects a new class definition from the {@code source}.
     *
     * @throws ClassNotFoundException If some error happened which prevented
     *                                class instantiation.
     */
    public static Class<?> inject(final String className, final String source)
        throws ClassNotFoundException
    {
        // Sanity
        if (className == null) {
            throw new ClassNotFoundException("Given a null class name");
        }
        if (source == null || source.isEmpty()) {
            throw new ClassNotFoundException("Source was null or missing");
        }

        // Instantiate the compiler with a diagnostic collector, which
        // stores any diagnostics (e.g. error messages) from compilation.
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final DiagnosticCollector<JavaFileObject> diagnostics =
            new DiagnosticCollector<>();

        // This will hold the compiled code
        final JavaByteObject byteObject;
        try {
            byteObject = new JavaByteObject(className);
        }
        catch (Exception e) {
            throw new ClassNotFoundException(
                "Could not create the raw source data for " + className,
                e
            );
        }

        // Instantiate the custom file manager, which recognizes the
        // compiled code stored in the  byteObject as a file.
        final StandardJavaFileManager standardFileManager =
            compiler.getStandardFileManager(diagnostics, null, null);
        final JavaFileManager fileManager =
            createFileManager(standardFileManager, byteObject);

        // Format the source code for compilation
        final JavaStringObject stringObject =
            new JavaStringObject(className, source);

        // Set up the compilation task. We will use -parameters option to store
        // formal parameter names of constructors and methods in the compiled
        // class.
        final JavaCompiler.CompilationTask task =
            compiler.getTask(null,                           // Writer
                             fileManager,
                             diagnostics,
                             Arrays.asList("-parameters"),   // Options
                             null,                           // Classes
                             Arrays.asList(stringObject));

        // Compile the class, throwing an exception if there are any errors
        if (!task.call()) {
            // Format the error message
            StringBuilder errorMsg = new StringBuilder();
            for (Diagnostic d : diagnostics.getDiagnostics()) {
                final int lineNumber = (int)d.getLineNumber();

                errorMsg.append("Compilation error: line ")
                        .append(lineNumber)
                        .append('\n')
                        .append(d.getMessage(null));

                // Print the erroneous line (lineNumber is 1-indexed)
                boolean found = false;
                for (int i = 0, line = 1;
                     i < source.length() && line <= lineNumber;
                     i++)
                {
                    final char c = source.charAt(i);
                    if (c == '\n') {
                        line++;
                    }
                    else if (line == lineNumber) {
                        if (!found) {
                            found = true;
                            errorMsg.append(" in:\n");
                        }
                        errorMsg.append(c);
                    }
                }
            }
            throw new ClassNotFoundException(
                errorMsg + "\n" +
                "(Make sure the className variable, \"" + className + "\", " +
                "and the class name in the source code are the same.)"
            );
        }

        // Clean up
        try {
            fileManager.close();
        }
        catch (IOException e) {
            // This should not happen since we don't do any IO
            throw new ClassNotFoundException(
                "Unexpected IOException when tidying up",
                e
            );
        }

        // Load the class from the byte object that holds the compiled code
        final ClassLoader inMemoryClassLoader = createClassLoader(byteObject);

        // And give it back
        return inMemoryClassLoader.loadClass(className);
    }
}

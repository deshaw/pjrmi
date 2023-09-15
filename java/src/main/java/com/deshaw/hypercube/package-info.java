/**
 * The {@code hypercube} package provides duck-types for {@code numpy.ndarray}s
 * and {@code numpy}-like functionality in Java.
 *
 * <p>The general intent of this package is to provide a {@code numpy}-like math
 * library which can be leveraged on the Java side. The classes as written so as
 * to make translating code between the Python and Java as simple as possible.
 * For examples of these translations, see the {@code hypercube.ipynb} Jupyter
 * Notebook.
 *
 * <p>The basic type is a {@link Hypercube} which wraps Java {@link Object}s.
 * There are type-specific sub-classes for working with primitive types, e.g.
 * {@link DoubleHypercube}. {@link Hypercube}s have a subset of the
 * {@code ndarray} functionality like slicing, reshaping, {@code __getitem__}
 * and {@code __setitem__}, iteration, and bulk reading and writing.
 *
 * <p>The {@link Hypercube} and {@link CubeMath} implementations are not as fast
 * as their {@code numpy} counterparts, but should be semantically equivalent.
 * Users can subclass the abstract classes to expose their own data. The classes
 * are intended to be thread-safe but operations are not atomic.
 *
 * <p><b>This code is experimental, may be buggy, and its APIs are likely to
 * change in future releases.</b>
 *
 * <p>We use cog.py to provide the different versions of various classes. In
 * order to update the source for them you need to do:</pre>
 *   cog.py -rc BlahBlah.java
 * <pre>
 *
 * See:<ul>
 *  <li>https://nedbatchelder.com/code/cog</li>
 *  <li>https://www.python.org/about/success/cog</li>
 * </ul>
 */
package com.deshaw.hypercube;

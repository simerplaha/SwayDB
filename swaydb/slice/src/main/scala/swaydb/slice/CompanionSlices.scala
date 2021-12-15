package swaydb.slice

import scala.reflect.ClassTag

/**
 * Companion implementation for [[Slices]].
 *
 * This is a trait because the [[Slices]] class itself is getting too
 * long even though inheritance such as like this is discouraged.
 */
trait CompanionSlices {

  @inline def apply[A: ClassTag](a: A): Slices[A] =
    new Slices(Array(Slice[A](a)))

  @inline def apply[A: ClassTag](a: A, b: A): Slices[A] =
    new Slices(Array(Slice[A](a, b)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A, r: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A, r: A, s: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A, r: A, s: A, t: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A, r: A, s: A, t: A, u: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)))

  @inline def apply[A: ClassTag](a: A, b: A, c: A, d: A, e: A, f: A, g: A, h: A, i: A, j: A, k: A, l: A, m: A, n: A, o: A, p: A, q: A, r: A, s: A, t: A, u: A, v: A): Slices[A] =
    new Slices(Array(Slice[A](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)))

  @inline def apply[A: ClassTag](slices: Array[Slice[A]]): Slices[A] =
    new Slices(slices)

}

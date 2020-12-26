'''
Intentionally slow functions.
The idea is to use these as things which are slow enough to be worth calculating in parallel and caching the results.

Rules:
  1. It has to work. So, while it is meant to be dumb, it will be smart enough to preconditioning, etc. if _required_.
  2. Every step has to make some useful progress towards the calculation. Don't _just_ burn cpu.
'''


# Don't import anything useful for doing things fast!
from math import pi, inf


def factorial(n):
    r = 1.
    for m in range(1,n+1):
        r = r * m
    return r


def pow(x,n):
    r = 1.
    for _ in range(n):
        r = r * x
    return r


def _preexp(x):
    '''Return y,n such that x = n * y with y near 1.'''
    n = 1
    while x > 4. / 3. or x < -4. / 3.:
        x = x / 2.
        n = n * 2
    return x, n
        

def _exp(x):
    pr = 0.
    r = 0.

    n = 0
    r = r + pow(x,n) / factorial(n)    

    n = 1
    while r != pr:
        pr = r
        r = r + pow(x,n) / factorial(n)
        n = n + 1
    return r


def exp(x):
    x, n = _preexp(x)
    return pow(_exp(x), n)


def sqrt(x):
    if x == 0.:
        return 0.
    
    pr = 0.
    r = x
    
    r = (r + x / r) / 2.
    while r != pr:
        pr = r
        r = (r + x / r) / 2.
    return r


def abs(z):
    return complex_to_real(sqrt(complex(z) * complex(z).conjugate()))
        

def _log(x):
    '''log(x) for x between 0 and 2'''
    pr = 0.
    r = 0.

    n = 1
    r = r + pow(-1., n + 1) * pow(x - 1., n) / n

    n = 2
    while r != pr:
        pr = r
        r = r + pow(-1., n + 1) * pow(x - 1., n) / n
        n = n + 1
    return r


def _prelog(x):
    '''Return a sequence of numbers, between 0 and 2, which multiply to x'''
    if x <= 0:
        raise ValueError('math domain error')
    elif x == 0.:
        return 1.
    elif x < 1 / 2. or x > 2.:
        return _prelog(sqrt(x)) + _prelog(sqrt(x))
    else:
        return [x]

    
def log(x):    
    r = 0.
    for y in _prelog(x):
       r = r + _log(y)
    return r


def complex_to_real(z):
    if z.imag != 0.:
        raise RuntimeError('Cannot convert ', z, 'to real')
    else:
        return z.real

    
def cos(theta):
    return complex_to_real((exponential(theta * 1.j) + exponential(-1.0 * theta * 1.j)).conjugate() / 2.0)


def sin(theta):                                                                    
    return complex_to_real((exponential(theta * 1.j) - exponential( -1.0 * theta * 1.j)).conjugate() / 2.0 / 1.j)


def normal_pdf(x, mu, sigma):
    return 1.0 / sigma / sqrt(2. * pi) * exp(-(x - mu) / sigma * (x - mu ) / sigma / 2.)


def nintegrate(f, args, xmin, xmax):
    # Divide range into N segmemts.
    # This requires N+1 points. Hence the + 1 in the list comps.
    #     [ xmin + i * (xmax - xmin) / N for i in range(N+1)] -> [0, 1/3, 2/3, 1]
    # Then do trapezoid rule.
    
    pn = 1
    pxs = [xmin + i * (xmax - xmin) / pn for i in range(pn + 1)]
    pfs = [f(x, *args) for x in pxs]
    pr = (pfs[0] / 2. + sum(pfs[1:-1]) + pfs[-1] / 2.) * (xmin + xmax) / pn
    pe = inf

    n = 2
    xs = [ xmin + i * (xmax - xmin) / n if i % 2 else pxs[i // 2] for i    in range(n+1) ]
    fs = [ f(x, *args)                  if i % 2 else pfs[i // 2] for i, x in enumerate(xs) ]
    r = (fs[0] / 2. + sum(fs[1:-1]) + fs[-1] / 2.) * (xmax - xmin) / n
    e = r - pr
        
    n = n * 2
    # We will quit when we get "perfect" or things are actually getting worse.
    while e != 0. and e * e < pe * pe:
        pxs = xs
        pfs = fs
        pr = r
        pe = e
        xs = [ xmin + i * (xmax - xmin) / n if i % 2 else pxs[i // 2] for i    in range(n+1) ]
        fs = [ f(x, *args)                  if i % 2 else pfs[i // 2] for i, x in enumerate(xs) ]
        r = (fs[0] / 2. + sum(fs[1:-1]) + fs[-1] / 2.) * (xmax - xmin) / n
        e = r - pr
        n = n * 2
    if e * e < pe * pe:
        return r
    else:
        return pr

    
def normal_cdf(x, mu, sigma):
    return _znormal_cdf((x - mu) / sigma)


def _znormal_cdf(x):
    if x >= 0.:
        return 0.5 + nintegrate(normal_pdf, (0., 1.), 0., x)
    else:
        return 0.5 - nintegrate(normal_pdf, (0., 1.), x, 0.5)


def main():
    import math
    import operator
    import functools
    for _ in range(1):
        #print(abs(-10.))
        #_ = exp(10.)
        #_ = cos(pi/3.)
        #_ = sin(pi/3.)
        #print(_preexp(10.))
        #print(exp(-10.))
        #print(functools.reduce(operator.mul, _prelog(13.), 1.))
        #print(exp(_log(0.5)))
        #print(log(10.))
        #print(_prelog(exp(-8.)))
        #print(log(0.00033546262785295476))
        #print(log(exp(-8.)))
        #print(sqrt(2.) * sqrt(2.))
        #print(normal_pdf(-17., 0., 1.))
        #print(nintegrate(  normal_pdf, (0., 1.), -1., 1.))
        #print(normal_cdf(0., 0., 1.))
        print(normal_cdf(1., 0., 1.))

        #import cProfile
        #cProfile.run("normal_cdf(1.0, 0., 1.)")

        
if __name__ == '__main__':
    main()


        
        
    
# This is too fast
# def erf(x):
#     pr = 0.
#     r = 0.

#     n = 0
#     r = r + pow(-1., n) * pow(x, 2 * n + 1) / factorial(n) / (2 * n + 1)

#     n = 1
#     while r != pr:
#         pr = r
#         r = r + pow(-1., n) * pow(x, 2 * n + 1) / factorial(n) / (2 * n + 1)
#         n = n + 1
#     return 2. / sqrt(pi) * r


#def normal_cdf(x, mu, sigma):
#    return 1. / 2. + lerf((x - mu) / sigma / sqrt(2.)) / 2.


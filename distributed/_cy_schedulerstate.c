#include "Python.h"

#if PY_MAJOR_VERSION < 3
PyMODINIT_FUNC initschedulerstate(void); /*proto*/

PyMODINIT_FUNC init_cy_schedulerstate(void); /*proto*/
PyMODINIT_FUNC init_cy_schedulerstate(void)
{
    return initschedulerstate();
}
#else
PyMODINIT_FUNC PyInit_schedulerstate(void); /*proto*/

PyMODINIT_FUNC PyInit__cy_schedulerstate(void); /* proto */
PyMODINIT_FUNC PyInit__cy_schedulerstate(void)
{
    return PyInit_schedulerstate();
}
#endif

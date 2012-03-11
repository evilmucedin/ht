#pragma once

#include <util/generic/ptr.h>

namespace NLFHT {
    // absolutly transpanter pointer wrapper
    // purpose is to add destructor to pointers
    template <class T>
    class TVolatilePointerWrapper : public TPointerBase<TVolatilePointerWrapper<T>, T> {
    public:
        TVolatilePointerWrapper(T* ptr = 0)
            : Ptr(ptr)
        {
        }

        inline operator T* () const {
            return Get();
        }
        inline T* volatile* operator& () {
            return &Ptr;
        }

        inline void Set(T* ptr) {
            Ptr = ptr;
        }
        inline T* Get() const {
            return Ptr;
        }
    private:
        T* volatile Ptr;
    };
};

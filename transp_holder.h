#pragma once

template <class Base, class T>
class TPointerCommon {
    public:
        typedef T TValueType;

        inline T* operator-> () const throw () {
            return AsT();
        }

        template <class C>
        inline bool operator== (const C& p) const throw () {
            return (p == AsT());
        }

        template <class C>
        inline bool operator!= (const C& p) const throw () {
            return (p != AsT());
        }

        inline bool operator! () const throw () {
            return 0 == AsT();
        }

    protected:
        inline T* AsT() const throw () {
            return (static_cast<const Base*>(this))->Get();
        }

        static inline T* DoRelease(T*& t) throw () {
            T* ret = t; t = 0; return ret;
        }
};

template <class Base, class T>
class TPointerBase: public TPointerCommon<Base, T> {
    public:
        inline T& operator* () const throw () {
            YASSERT(this->AsT());

            return *(this->AsT());
        }

        inline T& operator[] (size_t n) const throw () {
            YASSERT(this->AsT());

            return (this->AsT())[n];
        }
};

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

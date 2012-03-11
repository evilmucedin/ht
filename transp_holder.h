#pragma once

template <class Base, class T>
class PointerCommon
{
    public:
        typedef T ValueType;

        inline T* operator-> () const throw ()
        {
            return AsT();
        }

        template <class C>
        inline bool operator== (const C& p) const throw ()
        {
            return (p == AsT());
        }

        template <class C>
        inline bool operator!= (const C& p) const throw ()
        {
            return (p != AsT());
        }

        inline bool operator! () const throw ()
        {
            return 0 == AsT();
        }

    protected:
        inline T* AsT() const throw ()
        {
            return (static_cast<const Base*>(this))->Get();
        }

        static inline T* DoRelease(T*& t) throw ()
        {
            T* ret = t; t = 0; return ret;
        }
};

template <class Base, class T>
class PointerBase: public PointerCommon<Base, T>
{
    public:
        inline T& operator* () const throw ()
        {
            YASSERT(this->AsT());

            return *(this->AsT());
        }

        inline T& operator[] (size_t n) const throw ()
        {
            YASSERT(this->AsT());

            return (this->AsT())[n];
        }
};

namespace NLFHT {
    // absolutly transpanter pointer wrapper
    // purpose is to add destructor to pointers
    template <class T>
    class VolatilePointerWrapper : public PointerBase<VolatilePointerWrapper<T>, T>
    {
    public:
        VolatilePointerWrapper(T* ptr = 0)
            : m_Ptr(ptr)
        {
        }

        inline operator T* () const
        {
            return Get();
        }
        inline T* volatile* operator& ()
        {
            return &m_Ptr;
        }

        inline void Set(T* ptr)
        {
            m_Ptr = ptr;
        }
        inline T* Get() const
        {
            return m_Ptr;
        }
    private:
        T* volatile m_Ptr;
    };
};

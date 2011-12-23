#include "guards.h"
#include "lfht.h"

#include <limits>

namespace NLFHT {
    const AtomicBase TGuard::NO_OWNER = 0;
    const AtomicBase TGuard::NO_TABLE = std::numeric_limits<AtomicBase>::max();

    void TThreadGuardTable::RegisterTable(TLFHashTableBase* pTable) {
       if (!GuardTable) 
           GuardTable = new TGuardTable;
        (*GuardTable)[pTable] = pTable->AcquireGuard();
    }

    void TThreadGuardTable::ForgetTable(TLFHashTableBase* pTable) {
        TGuardTable::iterator it = GuardTable->find(pTable);
        it->second->Release();
        GuardTable->erase(it);

        if (GuardTable->empty())
            delete (TGuardTable*)GuardTable;
        GuardTable = (TGuardTable*)(0);
    }

    NLFHT_THREAD_LOCAL TThreadGuardTable::TGuardTable *TThreadGuardTable::GuardTable = 0;

    TGuard::TGuard(TGuardManager* parent) :
        Next(0),
        Parent(parent),
        Owner(NO_OWNER)
    {
        Init();    
    }

    void TGuard::Init() {
        AliveCnt = 0;
        KeyCnt = 0;

        GuardedTable = NO_TABLE;
        PTDLock = false;
        
#ifndef NDEBUG
        // JUST TO DEBUG
        LocalPutCnt = 0;
        LocalCopyCnt = 0;
        LocalDeleteCnt = 0;
        LocalLookUpCnt = 0;
        GlobalPutCnt = 0;
        GlobalGetCnt = 0;
#endif
    }

    void TGuard::Release() {
        Owner = NO_OWNER;
        AtomicAdd(Parent->KeyCnt, KeyCnt);
        AtomicAdd(Parent->AliveCnt, AliveCnt);
        Init();
    }

    TGuardManager::TGuardManager() :
        Head(0),
        AliveCnt(0),
        KeyCnt(0)
    {
    }

    TGuardManager::~TGuardManager() {
        // is to be called when all threads have called ForgetTable
        TGuard* current = Head;
        while (current) {
            TGuard* tmp = current;
            current = current->Next;
            
            VERIFY(tmp->Owner == TGuard::NO_OWNER, 
                   "Some thread haven't finish his work yet\n");
            delete tmp;
        }
    }

    TGuard* TGuardManager::AcquireGuard(size_t owner) {
        for (TGuard* current = Head; current; current = current->Next)
            if (current->Owner == TGuard::NO_OWNER)
                if (AtomicCas(&current->Owner, owner, TGuard::NO_OWNER))
                    return current;
        return CreateGuard(owner);
    }

    size_t TGuardManager::GetFirstGuardedTable() {
        size_t result = TGuard::NO_TABLE;
        for (TGuard* current = Head; current; current = current->Next)
            if (current->Owner != TGuard::NO_OWNER)
                result = Min(result, (size_t)current->GuardedTable);
        return result;
    }

    AtomicBase TGuardManager::TotalAliveCnt() {
        AtomicBase result = AliveCnt;
        for (TGuard* current = Head; current; current = current->Next)
            result += current->AliveCnt;
        return result;
    }

    AtomicBase TGuardManager::TotalKeyCnt() {
        AtomicBase result = KeyCnt;
        for (TGuard* current = Head; current; current = current->Next)
            result += current->KeyCnt;
        return result;
    }

    void TGuardManager::ZeroKeyCnt() {
        for (TGuard* current = Head; current; current = current->Next)
            current->KeyCnt = 0;
        KeyCnt = 0;
    }

    bool TGuardManager::CanPrepareToDelete() {
        for (TGuard* current = Head; current; current = current->Next)
            if (current->PTDLock)
                return false;
        return true;
    }

    // JUST TO DEBUG

    std::string TGuard::ToString() {
        std::stringstream tmp;
        tmp << "TGuard " << '\n'
            << "Owner " << Owner << '\n'
            << "KeyCnt " << KeyCnt << '\n'
            << "AliveCnt " << AliveCnt << '\n'; 
        return tmp.str();
    }

    std::string TGuardManager::ToString() {
        std::stringstream tmp;
        tmp << "GuardManager --------------\n";
        for (TGuard* current = Head; current; current = current->Next)
            tmp << current->ToString();
        tmp << "Common KeyCnt " << KeyCnt << '\n'
            << "Common AliveCnt " << AliveCnt << '\n';
        return tmp.str();
    }

    void TGuardManager::PrintStatistics(std::ostream& str) {
#ifndef NDEBUG
        size_t LocalPutCnt = 0, LocalCopyCnt = 0, LocalDeleteCnt = 0, LocalLookUpCnt = 0;
        size_t GlobalPutCnt = 0, GlobalGetCnt = 0;
        for (TGuard* current = Head; current; current = current->Next) {
            LocalPutCnt += current->LocalPutCnt;
            LocalCopyCnt += current->LocalCopyCnt;
            LocalDeleteCnt += current->LocalDeleteCnt;
            LocalLookUpCnt += current->LocalLookUpCnt;
            GlobalGetCnt += current->GlobalGetCnt;
            GlobalPutCnt += current->GlobalPutCnt;
            LocalPutCnt += current->LocalPutCnt;
        }

        str << "LocalPutCnt " << LocalPutCnt << '\n'
            << "LocalDeleteCnt " << LocalDeleteCnt << '\n'
            << "LocalCopyCnt " << LocalCopyCnt << '\n'
            << "LocalLookUpCnt " << LocalLookUpCnt << '\n'
            << "GlobalPutCnt " << GlobalPutCnt << '\n'
            << "GlobalGetCnt " << GlobalGetCnt << '\n';
#endif
    }

    TGuard* TGuardManager::CreateGuard(AtomicBase owner) {
        TGuard* guard = new TGuard(this);
        guard->Owner = owner;
        while (true) {
            guard->Next = Head;
            if (AtomicCas(&Head, guard, guard->Next))
               break;
        }
        return guard;
    }
};

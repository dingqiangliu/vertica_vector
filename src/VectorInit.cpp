/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024 -*- C++ -*- */
/*
 * Description: User Defined Scalar Function for vector_init
 *
 * Create Date: Dec. 12, 2024
 */

#include "Vertica.h"
#include "Arrays/Accessors.h"

using namespace Vertica;
using namespace std;

class VectorInit : public ScalarFunction
{
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        do {
            size_t argCount = argReader.getNumCols();
            if (argCount != 2)
                vt_report_error(0, "2 arguments (value, size) expected, but %d provided", argCount);

            const SizedColumnTypes argTypes = argReader.getTypeMetaData();
            if(!argTypes.getColumnType(1).isInt())
                vt_report_error(0, "The 2nd argument size should be integer");

            const vint size = argReader.getIntRef(1);
            if(size < 0)
            {
                    vt_report_error(1, "the 2nd argument size should be no less than 0 but %d provided", size);
                    return;
            }

            const BaseDataOID typeOid = argTypes.getColumnType(0).getUnderlyingType();
            Array::ArrayWriter outArray = resWriter.getArrayRef(0);
            bool isNull = argReader.isNull(0);
            for(vint i = 0; i < size; i++)
            {
                if(isNull)
                    outArray->setNull();
                else if(typeOid == BoolOID)
                    outArray->setBool(argReader.getBoolRef(0));
                else if(typeOid == Int8OID)
                    outArray->setInt(argReader.getIntRef(0));
                else if(typeOid == Float8OID)
                    outArray->setFloat(argReader.getFloatRef(0));
                else if(typeOid == NumericOID)
                    outArray->getNumericRef().copy(argReader.getNumericPtr(0));
                else if(typeOid == DateOID)
                    outArray->setDate(argReader.getDateRef(0));
                else if(typeOid == TimestampOID)
                    outArray->setTimestamp(argReader.getTimestampRef(0));
                else if(typeOid == TimestampTzOID)
                    outArray->setTimestampTz(argReader.getTimestampTzRef(0));
                else if(typeOid == TimeOID)
                    outArray->setTime(argReader.getTimeRef(0));
                else if(typeOid == TimeTzOID)
                    outArray->setTimeTz(argReader.getTimeTzRef(0));
                else if(typeOid == IntervalOID)
                    outArray->setInterval(argReader.getIntervalRef(0));
                else if(typeOid == IntervalYMOID)
                    outArray->setIntervalYM(argReader.getIntervalYMRef(0));
                else if(typeOid == UuidOID)
                    outArray->getUuidRef().copy(argReader.getUuidRef(0));
                else {
                    vt_report_error(1, "Unsupported type [%s] of column [%zu]", 
                                    argTypes.getColumnType(0).getTypeStr(), 1);
                }

                outArray->next();
            }
            outArray.commit();

            resWriter.next();
        } while (argReader.next());
     }
};


class VectorInitFactory : public ScalarFunctionFactory
{
    ScalarFunction *createScalarFunction(ServerInterface &interface) override
    {
        return vt_createFuncObject<VectorInit>(interface.allocator);
    }


    virtual void getPrototype(ServerInterface &srvInterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }


    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes)
    {
        size_t argCount = argTypes.getColumnCount();
        if (argCount != 2)
            vt_report_error(0, "2 arguments (value, size) expected, but %d provided", argCount);

        const VerticaType &inputType = argTypes.getColumnType(0);
        BaseDataOID baseOid = inputType.getUnderlyingType();
        switch(baseOid)
        {
            case BoolOID:
            case Int8OID:
            case Float8OID:
            case NumericOID:
            case DateOID:
            case TimestampOID:
            case TimestampTzOID:
            case TimeOID:
            case TimeTzOID:
            case IntervalOID:
            case IntervalYMOID:
            case UuidOID: break;
            default: vt_report_error(1, "Unsupported type [%s] of column [%zu]", inputType.getTypeStr(), 1);
        }

        VerticaType arrayType(getArrayOid(baseOid), Typemod::array(-1, inputType.getTypeMod()));
        returnTypes.addArg(arrayType);
    }


public:
    VectorInitFactory() 
    {
        vol = STABLE;
        strict = STRICT;
    }
};


RegisterFactory(VectorInitFactory);

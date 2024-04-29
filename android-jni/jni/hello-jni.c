#include <string.h>
#include <jni.h>
#include <stdlib.h>

extern void FreeCString(char* jsonStrPtr);
extern char* JSONCall(char* jsonStrPtr);

jstring
Java_com_example_titan_1app_HelloJni_JSONCall( JNIEnv* env,
                                                  jobject thiz, jstring args)
{
#if defined(__arm__)
  #if defined(__ARM_ARCH_7A__)
    #if defined(__ARM_NEON__)
      #if defined(__ARM_PCS_VFP)
        #define ABI "armeabi-v7a/NEON (hard-float)"
      #else
        #define ABI "armeabi-v7a/NEON"
      #endif
    #else
      #if defined(__ARM_PCS_VFP)
        #define ABI "armeabi-v7a (hard-float)"
      #else
        #define ABI "armeabi-v7a"
      #endif
    #endif
  #else
   #define ABI "armeabi"
  #endif
#elif defined(__i386__)
   #define ABI "x86"
#elif defined(__x86_64__)
   #define ABI "x86_64"
#elif defined(__mips64)  /* mips64el-* toolchain defines __mips__ too */
   #define ABI "mips64"
#elif defined(__mips__)
   #define ABI "mips"
#elif defined(__aarch64__)
   #define ABI "arm64-v8a"
#else
   #define ABI "unknown"
#endif

    const char *sx;
    char* goResult;
    jstring result;

    /* Get the UTF-8 characters that represent our java string */
    sx = (*env)->GetStringUTFChars(env, args, NULL);
    
    goResult = JSONCall((char*)sx);
    (*env)->ReleaseStringUTFChars(env, args, sx);

    result = (*env)->NewStringUTF(env, goResult);
    FreeCString(goResult);
    
    return result;
}

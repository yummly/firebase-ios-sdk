//
//  FIRExperimentDelegate.h
//  Pods
//
//  Created by Alexandra Lovin on 25.02.2022.
//

#ifndef FIRExperimentDelegate_h
#define FIRExperimentDelegate_h

@protocol FIRExperimentDelegate <NSObject>
- (void)handleExperimentStarted:(NSString * __nullable)experimentId variantId:(NSString * __nullable)variantId origin:(NSString * __nullable)origin ;
@end

#endif /* FIRExperimentDelegate_h */
